#define PERL_NO_GET_CONTEXT 1
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include <mysql.h>

#ifndef hv_deletes
# define hv_deletes(hv, key, flags) \
    hv_delete((hv), ("" key ""), (sizeof(key)-1), (flags))
#endif

#define prepare_new_result_accumulator(maria) STMT_START { \
    maria->query_results ? SvREFCNT_dec(maria->query_results) : NULL; \
    maria->query_results = MUTABLE_SV(newAV()); \
} STMT_END

typedef struct sql_config {
    /* passed to mysql_real_connect(_start) */
    const char* username;
    const char* hostname;
    const char* unix_socket;
    const char* database;
    unsigned int  port;
    unsigned long client_opts;

    /* We may temporarily hold a pointer to the SV that holds the
     * password.
     * If we have auto-reconnect enabled, then this will hold
     * a real SV that we have ownership to, but that is not
     * yet implemented, since it seems like a bad idea to me.
     *
     * Instead, this should never be assigned to normally;
     * rather, it should be temporarily given values via
     * one of Perl's save-and-restore-at-end-of-scope values.
     * Also note that we'll go through some lengths to
     * not needlessly copy the password out of the SV, but more
     * on that later.
     */
    SV* password_temp;

    const char* charset_name;

    /* SSL */
    const char* ssl_key;
    const char* ssl_cert;
    const char* ssl_ca;
    const char* ssl_capath;
    const char* ssl_cipher;
} sql_config;

typedef struct MariaDB_client {
    MYSQL*     mysql;
    MYSQL_RES* res;

    sql_config* config;

    SV* query_results;
    SV* query_sv;

    /* conveniences */
    int socket_fd;
    unsigned long thread_id;

    /* For the state machine */
    bool is_cont;   /* next operation must be a _cont */
    int current_state;
    int last_status;

    /* Should we buffer the entire resultset once the query is done,
     * or fetch it row-by-row?
     */
    bool store_query_result;

    /* DESTROY has been called, we already freed the memory we allocated */
    bool destroyed;
} MariaDB_client;

#define dMARIA MariaDB_client* maria; STMT_START { \
    {\
        MAGIC *mg;\
        if (sv_isobject(self) && SvTYPE(SvRV(self)) == SVt_PVHV && \
            (mg = mg_findext(SvRV(self), PERL_MAGIC_ext, &maria_vtable)) != 0) \
        { maria = (MariaDB_client*) mg->mg_ptr; }\
        else { \
            croak("%"SVf" is not a valid Maria::NonBlocking object", self); \
        } \
    } \
} STMT_END

/* Stolen from http://stackoverflow.com/a/37277144 */
#define NAMES C(DISCONNECTED)C(STANDBY)C(CONNECT)C(QUERY)C(ROW_FETCH)C(FREE_RESULT)C(PING)C(STORE_RESULT)
#define C(x) STATE_##x,
enum color { NAMES STATE_END };
#undef C
#define C(x) #x,
const char * const state_to_name[] = { NAMES };
#undef C

const char* const cont_to_name[] = { "cont", "run_query_cont", "ping_cont", "connect_cont" };

void
THX_free_our_config_items(pTHX_ sql_config* config)
#define free_our_config_items(c) THX_free_our_config_items(aTHX_ c)
{
    Safefree(config->username);
    Safefree(config->hostname);
    Safefree(config->unix_socket);
    Safefree(config->database);
    Safefree(config->charset_name);
    Safefree(config->ssl_key);
    Safefree(config->ssl_cert);
    Safefree(config->ssl_ca);
    Safefree(config->ssl_capath);
    Safefree(config->ssl_cipher);
    return;
}

void
THX_disconnect_generic(pTHX_ MariaDB_client* maria)
#define disconnect_generic(maria) THX_disconnect_generic(aTHX_ maria)
{
    if ( maria->query_sv ) {
        SvREFCNT_dec(maria->query_sv);
        maria->query_sv = NULL;
    }

    if ( maria->res ) {
        /* do a best attempt... */
        int status = mysql_free_result_start(maria->res);
        maria->res = NULL; /* memory might leak here if status != 0 */
        if ( status ) {
            /* Damn.  Would've blocked trying to free the result.  Not much we can do */
            croak("Could not free statement handle without blocking.");
        }
    }

    if ( maria->mysql ) {
        mysql_close(maria->mysql);
        Safefree(maria->mysql);
        maria->mysql = NULL;
    }

    maria->is_cont       = FALSE;
    maria->current_state = STATE_DISCONNECTED;

    return;
}

static int
free_mariadb_handle(pTHX_ SV* sv, MAGIC *mg)
{
    MariaDB_client* maria = (MariaDB_client*)mg->mg_ptr;
    sql_config *config;

    if ( !maria || maria->destroyed ) {
        return 0;
    }

    config = maria->config;

    maria->destroyed = 1;

    disconnect_generic(maria);

    /* Free all the config items we may have allocated */
    free_our_config_items(config);

    Safefree(maria->config); /* free the memory we Newxz'd before */
    maria->config = NULL;
    Safefree(maria); /* free the memory we Newxz'd before */
    /* mg will be freed by our caller */
    mg->mg_ptr = NULL;

    return 0;
}

static MGVTBL maria_vtable = { .svt_free = free_mariadb_handle };

const char*
THX_fetch_pv_try_not_to_copy_buffer(pTHX_ SV* password_sv)
#define fetch_pv_try_not_to_copy_buffer(a) \
    THX_fetch_pv_try_not_to_copy_buffer(aTHX_ a)
{
    const char* password = NULL;

    if ( !password_sv || !SvOK(password_sv) ) {
        return NULL;
    }

    if ( SvPOK(password_sv) && !SvGMAGICAL(password_sv) ) {
        /* Er... this is probably a bad assumption on my part.
         * Basically, I'm guessing that SvPOK means we are good
         * to just access the pv buffer directly.
         * This is the ideal situation!
         */
        password = SvPVX_const(password_sv);
    }
    else {
        /* magic and/or weird shit */
        password = SvPV_nolen(password_sv);
    }

    return password;
}

const char*
THX_fetch_password_try_not_to_copy_buffer(pTHX_ MariaDB_client *maria)
#define fetch_password_try_not_to_copy_buffer(a) \
    THX_fetch_password_try_not_to_copy_buffer(aTHX_ a)
{
    return fetch_pv_try_not_to_copy_buffer(maria->config->password_temp);
}

int
THX_add_row_to_results(pTHX_ MariaDB_client *maria, MYSQL_ROW row)
#define add_row_to_results(a,b) THX_add_row_to_results(aTHX_ a,b)
{
    AV *query_results;
    AV *row_results;
    int field_count;
    unsigned long *lengths;
    SSize_t i = 0;

    if (!maria->query_results) {
        /* we can reach this code if the results are requested
         * whilst we are still gathering rows from the server.
         */
        maria->query_results = MUTABLE_SV(newAV());
    }

    query_results = MUTABLE_AV(maria->query_results);

    field_count   = mysql_field_count(maria->mysql);
    lengths       = mysql_fetch_lengths(maria->res);

    row_results   = newAV();
    /* do the push now, in case somehow the av_push inside the loop dies
     * since that way we will avoid leaking the row_results AV
     */
    av_push(query_results, newRV_noinc(MUTABLE_SV(row_results)));

    /* we know how many columns the row has, so pre-extend row_results */
    av_extend(row_results, field_count);

    for ( i = 0; i < field_count; i++ ) {
        /* if the col is a NULL, then that will already be there,
         * since we pre-extended the array before.  So avoid doing
         * that store entirely
         */
        if ( row[i] ) {
            SV *col_data = newSVpvn(row[i], (STRLEN)lengths[i]);
            /* ownership of the col_data refcount goes to row_results */
            av_store(row_results, i, col_data);
        }
    }

    return 1;
}

#define store_in_self(pv, sv) hv_stores(MUTABLE_HV(SvRV(self)),pv,sv)

void
THX_usual_post_connect_shenanigans(pTHX_ SV* self)
#define usual_post_connect_shenanigans(s) \
    THX_usual_post_connect_shenanigans(aTHX_ s)
{
    dMARIA;

    maria->is_cont         = FALSE;
    maria->current_state   = STATE_STANDBY;

    /* Grab socket_fd outside. */
    maria->thread_id = mysql_thread_id(maria->mysql);
    (void)store_in_self(
        "mysql_thread_id",
        newSVnv(maria->thread_id)
    );

    return;
}

#define maybe_init_mysql_connection(mysql) STMT_START {           \
    if ( !mysql ) {                                               \
        my_bool reconnect = 0;                                    \
        Newxz(mysql, 1, MYSQL);                                   \
        mysql_init(mysql);                                        \
        mysql_options(mysql, MYSQL_OPT_NONBLOCK, 0);              \
        mysql_options(mysql, MYSQL_OPT_RECONNECT, &reconnect);    \
    }                                                             \
} STMT_END


int
THX_do_work(pTHX_ SV* self, IV event)
#define do_work(self, event) THX_do_work(aTHX_ self, event)
{
    dMARIA;
    int err             = 0;
    int status          = 0;
    int state           = maria->current_state;
    int state_for_error = STATE_STANDBY; /* query errors */
    sql_config *config   = maria->config;
    const char* errstring;

#define have_password_in_memory(maria) (maria && maria->config && maria->config->password_temp && SvOK(maria->config->password_temp))

    while ( 1 ) {
        if ( err )
            break;

        if ( status )
            break;

        if ( state == STATE_STANDBY && !maria->query_sv )
            break;

        if ( state == STATE_DISCONNECTED && !have_password_in_memory(maria) )
            break;

        /*warn("<%d><%s><%d>\n", maria->socket_fd, state_to_name[maria->current_state], maria->is_cont);*/
        switch ( state ) {
            case STATE_STANDBY:
                if ( maria->query_sv ) {
                    /* we have a query sv saved, so go ahead and run it! */
                    state = STATE_QUERY;
                }
                /* Otherwise, the loop will just end */
                break;
            case STATE_DISCONNECTED:
                /* If we still have the password around, then we can try
                 * connecting -- otherwise, time to end this suffering.
                 */
                if ( have_password_in_memory(maria) ) {
                    state = STATE_CONNECT;
                }
                else {
                    if ( maria->query_sv ) {
                        err       = 1;
                        errstring = "Disconnected and no password in memory to reconnect, nothing to do!";
                    }
                }
                break;
            case STATE_CONNECT:
            {
                MYSQL *mysql_ret = 0;
                if ( !maria->is_cont ) {
                    const char *password =
                        fetch_password_try_not_to_copy_buffer(maria);

                    if ( !password ) {
                        err       = 1;
                        errstring = "No password currently in memory! Probably a good thing!";
                        break;
                    }

                    status = mysql_real_connect_start(
                                 &mysql_ret,
                                 maria->mysql,
                                 maria->config->hostname,
                                 maria->config->username,
                                 password,
                                 maria->config->database,
                                 maria->config->port,
                                 maria->config->unix_socket,
                                 /* XXX TODO: CLIENT_REMEMBER_OPTIONS
                                  * without it, a connect_cont following
                                  * an error -- like a bad password --
                                  * will cause segfaults, presumably because
                                  * the connection won't have the async
                                  * context set.
                                  */
                                 CLIENT_REMEMBER_OPTIONS
                                    |
                                 maria->config->client_opts
                            );
                    maria->socket_fd = mysql_get_socket(maria->mysql);
                    (void)store_in_self(
                        "mysql_socket_fd",
                        newSViv(maria->socket_fd)
                    );
                }
                else {
                    status = mysql_real_connect_cont(
                                &mysql_ret,
                                maria->mysql,
                                event
                             );
                    event = 0;
                }

                if (mysql_errno(maria->mysql) > 0) {
                    maria->is_cont  = FALSE;
                    err             = 1;
                    errstring       = mysql_error(maria->mysql);
                    state_for_error = STATE_DISCONNECTED;
                    break;
                }

                if ( status ) {
                    /* need to wait and call _cont */
                    /* TODO mysql_errno here? */
                    maria->is_cont = TRUE;
                }
                else {
                    /* connected! */
                    usual_post_connect_shenanigans(self);
                    state = STATE_STANDBY;
                }
                break;
            }
            case STATE_QUERY:
                if ( maria->is_cont ) {
                    status = mysql_real_query_cont(&err, maria->mysql, event);
                    event = 0;
                }
                else {
                    SV* query_sv   = maria->query_sv;
                    STRLEN query_len;
                    char* query_pv = SvPV(query_sv, query_len);
                    status = mysql_real_query_start(
                                &err,
                                maria->mysql,
                                query_pv,
                                query_len
                             );
                    /* Release this */
                    SvREFCNT_dec(query_sv);
                    maria->query_sv = NULL;
                }

                if ( err ) {
                    /* Probably should do more here, like resetting the state */
                    maria->is_cont = FALSE;
                    state          = STATE_STANDBY;

                    errstring = mysql_error(maria->mysql);
                }
                else if ( status ) {
                    maria->is_cont = TRUE; /* need to wait on the socket */
                }
                else {
                    /* query finished */
                    maria->is_cont = FALSE; /* hooray! */

                    if ( maria->store_query_result ) {
                        state = STATE_STORE_RESULT;
                    }
                    else {
                        maria->res     = mysql_use_result(maria->mysql);

                        if ( maria->res ) {
                            /* query returned and we have results,
                             * start fetching!
                             */
                            state = STATE_ROW_FETCH;
                        }
                        else if ( mysql_field_count(maria->mysql) == 0 ) {
                            /* query succeeded but returned no data */
                            /* TODO might want to store affected rows? */
                            /*rows = mysql_affected_rows(maria->mysql);*/
                            /*prepare_new_result_accumulator(maria);*/ /* empty results */
                            state = STATE_STANDBY;
                        }
                        else {
                            /* Error! */
                            err       = 1;
                            errstring = mysql_error(maria->mysql);
                            /* maria->res is NULL if we get here, so no need
                             * to go and free that
                             */
                            state  = STATE_STANDBY;
                        }
                    }
                }
                break;
            case STATE_STORE_RESULT:
                if ( maria->is_cont ) {
                    status = mysql_store_result_cont(&(maria->res), maria->mysql, event);
                    event  = 0;
                }
                else {
                    status = mysql_store_result_start(&(maria->res), maria->mysql);
                }

                if ( status ) {
                    maria->is_cont = TRUE;
                }
                else {
                    /* we are done getting the results! */
                    maria->is_cont = FALSE;
                    if ( mysql_errno(maria->mysql) > 0 ) {
                        err       = 1;
                        errstring = mysql_error(maria->mysql);
                    }
                    else {
                        if ( !maria->res ) {
                            /* query was successful but returned nothing */
                            /* nothing to do, really */
                        }
                        else if ( mysql_field_count(maria->mysql) == 0 ) {
                            /* same */
                        }
                        else {
                            /* query was successful and we have all the results in memory.  Put them in perl! */
                            while (1) {
                                /* this will never block */
                                MYSQL_ROW row = mysql_fetch_row(maria->res);
                                if ( !row )
                                    break;

                                add_row_to_results(maria, row);
                            }
                        }
                    }
                    state = STATE_FREE_RESULT;
                }

                break;
            case STATE_FREE_RESULT:
                if ( maria->is_cont ) {
                    status = mysql_free_result_cont(maria->res, event);
                    event = 0;
                }
                else {
                    status = mysql_free_result_start(maria->res);
                }

                if (!status) {
                    /* freed the result */
                    maria->res     = NULL;
                    maria->is_cont = FALSE;
                    state = STATE_STANDBY; /* back to standby! */
                }
                else {
                    maria->is_cont = TRUE;
                }
                break;
            case STATE_ROW_FETCH:
            {
                MYSQL_ROW row = 0;
                if ( maria->is_cont ) {
                    status = mysql_fetch_row_cont(&row, maria->res, event);
                    event  = 0;
                    if (!status) {
                        if ( row ) {
                            add_row_to_results(maria, row);
                        }
                        else {
                            if ( mysql_errno(maria->mysql) > 0 ) {
                                err       = 1;
                                errstring = mysql_error(maria->mysql);
                            }
                            state = STATE_FREE_RESULT;
                        }
                        maria->is_cont = FALSE;
                    }
                }
                else {
                    do {
			            row    = 0;
                        status = mysql_fetch_row_start(&row, maria->res);
                        if (!status) {
                            if ( row ) {
                                add_row_to_results(maria, row);
                            }
                            else if ( mysql_errno(maria->mysql) > 0 ) {
                                /* Damn... We got an error while fetching
                                 * the rows.  Need to free the resultset
                                 */
                                err       = 1;
                                errstring = mysql_error(maria->mysql);
                                state     = STATE_FREE_RESULT;
                            }
                        }
                    } while (!status && row);

                    if ( status ) {
                        /* TODO: can mysql_errno() be > 0 here? Do we need to check? */
                        maria->is_cont = TRUE; /* need to wait on the socket */
                    }
                    else if (!row) {
                        /* all rows read, so free the result
                         * docs say we could use mysql_free_result
                         * and it should not block, but I'm having none of it.
                         */
                        state = STATE_FREE_RESULT;
                    }
                    /*
                        else, strangely enough, we need to call ourselves again
                    */
                }
                break;
            }
            case STATE_PING:
            {
                int ret;
                if ( maria->is_cont ) {
                    status = mysql_ping_cont(&ret, maria->mysql, event);
                    event  = 0;
                }
                else {
                    status = mysql_ping_start(&ret, maria->mysql);
                }

                if ( status ) {
                    maria->is_cont = TRUE;
                }
                else {
                    /* Ping finished! */
                    maria->is_cont       = FALSE;
                    state                = STATE_STANDBY;

                    if ( ret ) {
                        const char* ping_error = mysql_error(maria->mysql);
                        maria->query_results = newSVpvn( ping_error, strlen(ping_error) );
                    }
                    else {
                        maria->query_results = &PL_sv_no;
                    }
                }
                break;
            }
            default:
                err       = 1;
                errstring = "Should never happen! Invalid state";
        } /* end of switch (state) */
    }

    /*
     * We croak outside of the while, to give it a chance
     * to free the result
     */
    if ( err ) {
        maria->is_cont       = FALSE;
        /* TODO this is a terrible state to end in if we failed to connect */
        maria->current_state = state_for_error;
        maria->last_status   = 0;

        if ( maria->query_sv ) {
            SvREFCNT_dec(maria->query_sv);
            maria->query_sv = NULL;
        }

        if (!errstring)
            errstring = "Unknown MySQL error";
        croak("%s", errstring);
    }

    maria->current_state = state;
    maria->last_status   = status;

    return status;
}

const char*
THX_easy_arg_fetch(pTHX_ HV *hv, char * pv, STRLEN pv_len, bool required)
#define easy_arg_fetch(hv, s, b) THX_easy_arg_fetch(aTHX_ hv, s, sizeof(s)-1, b)
{
    SV** svp;
    const char *res = NULL;

    if ((svp = hv_fetch(hv, pv, pv_len, FALSE))) {
        if ( SvOK(*svp) ) {
            STRLEN len;
            res = SvPV_const(*svp, len);
        }
        /* it can stay NULL for undef */
    }
    else if ( required ) {
        croak("No %s given to ->connect / ->connect_start!", pv);
    }
    return res;
}

/* TODO should be called positive integer */
void
THX_mysql_opt_integer(pTHX_ MariaDB_client* maria, HV* hv, const char* str, STRLEN str_len, enum mysql_option option )
#define mysql_opt_integer(m, h, s, o) \
    THX_mysql_opt_integer(aTHX_ m, h, s, sizeof(s)-1, o)
{
    int value;
    SV** svp = hv_fetch(hv, str, str_len, FALSE);

    if ( !svp || !*svp || !SvOK(*svp) )
        return;

    value = SvIV(*svp);
    if ( value ) /* huh... */
        mysql_options(
            maria->mysql,
            option,
            (const char*)&value
        );

    return;
}

void
THX_unpack_config_from_hashref(pTHX_ SV* self, HV* args)
#define unpack_config_from_hashref(a,b) THX_unpack_config_from_hashref(aTHX_ a,b)
{
    SV** svp;
    dMARIA;
    sql_config *config = maria->config;

    /*
        With this code:
            $maria->connect({user => "foo"});
            $maria->disconnect;
            $maria->connect({user => "bar"});
        If we don't clear config items, we could end up
        with a mix of both configs.  And if we don't free
        them, we leak memory.
    */
    free_our_config_items(config);

    config->hostname     = savepv(easy_arg_fetch(args, "host",        TRUE));
    config->username     = savepv(easy_arg_fetch(args, "user",        TRUE));
    config->database     = savepv(easy_arg_fetch(args, "database",    FALSE));
    config->unix_socket  = savepv(easy_arg_fetch(args, "unix_socket", FALSE));

    config->charset_name = savepv(easy_arg_fetch(args, "charset", FALSE));

#define FETCH_FROM_HV(s) \
    ((svp = hv_fetch(args, s, sizeof(s)-1, FALSE)) && *svp && SvOK(*svp))

    config->port = FETCH_FROM_HV("port") ? SvIV(*svp) : 0;

    maria->store_query_result = FETCH_FROM_HV("mysql_use_results")
                                    ? cBOOL(SvTRUE(*svp) ? FALSE : TRUE)
                                    : TRUE;

    /* TODO: DBD::mysql compat mysql_enable_utf8 / mysql_enable_utf8mb4 */
    if ( config->charset_name && strlen(config->charset_name) )
        mysql_options(
            maria->mysql,
            MYSQL_SET_CHARSET_NAME,
            config->charset_name
        );

    if ( config->unix_socket && strlen(config->unix_socket) ) {
        const int xxx = MYSQL_PROTOCOL_SOCKET;
        mysql_options(
            maria->mysql,
            MYSQL_OPT_PROTOCOL,
            &xxx
        );
    }

    /* Going to follow DBD::mysql's naming scheme for these */

    if ( FETCH_FROM_HV("mysql_init_command") && SvTRUE(*svp) ) {
        const char* init_command = SvPV_nolen_const(*svp);
        mysql_options(
            maria->mysql,
            MYSQL_INIT_COMMAND,
            init_command
        );
    }

    if ( FETCH_FROM_HV("mysql_compression") ) {
        mysql_options(
            maria->mysql,
            MYSQL_OPT_COMPRESS,
            NULL /* unused */
        );
    }


#define my_mysql_opt_integer(a,b) mysql_opt_integer(maria, args, a, b)

    my_mysql_opt_integer("mysql_connect_timeout", MYSQL_OPT_CONNECT_TIMEOUT);
    my_mysql_opt_integer("mysql_write_timeout", MYSQL_OPT_WRITE_TIMEOUT);
    my_mysql_opt_integer("mysql_read_timeout", MYSQL_OPT_READ_TIMEOUT);

#undef my_mysql_opt_integer

    if ( FETCH_FROM_HV("ssl") ) {
        my_bool reject_unauthorized = 0;
        bool use_default_ciphers    = TRUE;
        HV *ssl;

        if ( !SvROK(*svp) || SvTYPE(SvRV(*svp)) != SVt_PVHV ) {
            /* TODO != SVt_PVHV? Do you even magic?? */
            croak("ssl argument should point to a hashref");
        }
        else {
            ssl = MUTABLE_HV(SvRV(*svp));
        }

#define ssl_config_set(s) STMT_START {                           \
    config->ssl_##s = savepv(easy_arg_fetch(ssl, #s, FALSE));    \
    if ( !config->ssl_##s ) config->ssl_##s = "";                \
} STMT_END

        ssl_config_set(key);
        ssl_config_set(cert);
        ssl_config_set(ca);
        ssl_config_set(capath);
        ssl_config_set(cipher);

        if ( config->ssl_cipher && strlen(config->ssl_cipher) ) {
            use_default_ciphers = FALSE;
        }

        if ( (svp = hv_fetchs(ssl, "reject_unauthorized", FALSE)) && *svp )
            reject_unauthorized = cBOOL(SvTRUE(*svp)) ? 1 : 0;

/*
        mysql_options(
            maria->mysql,
            MYSQL_OPT_SSL_MODE,
            reject_unauthorized
                ? &SSL_MODE_REQUIRED
                : &SSL_MODE_PREFERRED
        );
*/

        /* What a fucking mess.  This is deprecated and will be
         * removed in MySQL 8.  But the ssl modes enum isn't available
         * everywhere, so you can't use Oracle's recommended way. Bah.
         * TODO fix all of this ffs
         */

        mysql_options(
            maria->mysql,
            MYSQL_OPT_SSL_VERIFY_SERVER_CERT,
            &reject_unauthorized
        );

/* aka somewhat modern shit */
#define DEFAULT_CIPHERS "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES128-SHA256:ECDHE-RSA-AES128-SHA256"

        mysql_ssl_set(
            maria->mysql,
            config->ssl_key,
            config->ssl_cert,
            config->ssl_ca,
            config->ssl_capath,
            use_default_ciphers
                ? DEFAULT_CIPHERS
                : config->ssl_cipher
        );
    }

#undef FETCH_FROM_HV

}

MODULE = MariaDB::NonBlocking PACKAGE = MariaDB::NonBlocking

PROTOTYPES: DISABLE

BOOT:
{
    /* On boot, let's create a couple of constants for
     * perl to use
     */
    HV *stash = gv_stashpvs("MariaDB::NonBlocking", GV_ADD);

    CV *wait_read_cv    = newCONSTSUB(stash, "MYSQL_WAIT_READ", newSViv(MYSQL_WAIT_READ));

    CV *wait_write_cv   = newCONSTSUB(stash, "MYSQL_WAIT_WRITE", newSViv(MYSQL_WAIT_WRITE));

    CV *wait_except_cv  = newCONSTSUB(stash, "MYSQL_WAIT_EXCEPT", newSViv(MYSQL_WAIT_EXCEPT));

    CV *wait_timeout_cv = newCONSTSUB(stash, "MYSQL_WAIT_TIMEOUT", newSViv(MYSQL_WAIT_TIMEOUT));

    PERL_UNUSED_VAR(wait_read_cv);
    PERL_UNUSED_VAR(wait_write_cv);
    PERL_UNUSED_VAR(wait_except_cv);
    PERL_UNUSED_VAR(wait_timeout_cv);
}

SV*
init(SV* classname)
CODE:
{
    SV* self;

    MariaDB_client *maria;
    sql_config *config;
    Newxz(maria, 1, MariaDB_client);
    Newxz(config, 1, sql_config);

    maria->config        = config;
    maria->query_results = NULL;
    maria->current_state = STATE_DISCONNECTED;
    maria->socket_fd     = -1;
    maria->thread_id     = -1;
    maria->store_query_result = TRUE;

    maybe_init_mysql_connection(maria->mysql);

    /* create a reference to a hash, bless into $classname, then add the
     * magic we need
     */
    RETVAL = newRV_noinc(MUTABLE_SV(newHV()));
    sv_bless(RETVAL, gv_stashsv(classname, GV_ADD));
    sv_magicext(
        SvRV(RETVAL),
        SvRV(RETVAL),
        PERL_MAGIC_ext,
        &maria_vtable,
        (char*) maria,
        0
    );
}
OUTPUT: RETVAL

int
mysql_socket_fd(SV* self)
CODE:
    dMARIA;
    RETVAL = maria->socket_fd;
OUTPUT: RETVAL

IV
connect_start(SV* self, HV* args)
CODE:
{
    HV *inner_self  = MUTABLE_HV(SvRV(self));
    SV **svp        = hv_fetchs(args, "password", FALSE);

    dMARIA;

	if (!svp || !*svp)
	    croak("No password given to ->connect_start");

    if ( maria->current_state != STATE_DISCONNECTED )
        croak("Cannot call connect_start because the current internal state says we are on %s but we need to be in %s", state_to_name[maria->current_state], state_to_name[STATE_DISCONNECTED]);

    /* might be uninitialized due to a previous disconnect */
    maybe_init_mysql_connection(maria->mysql);
    unpack_config_from_hashref(self, args);

    /*
       What follows is the equivalent of
         local $self->{config}{password} = $args->{password};
       We do this so that the password PV is never copied anywhere;
       we later get it out of the string with SvPVX (if possible)
       and pass that directly to MySQL.
    */

    SAVEGENERICSV(maria->config->password_temp);
    maria->config->password_temp = SvREFCNT_inc(*svp);

    RETVAL = do_work(self, 0);
}
OUTPUT: RETVAL

IV
ping_start(SV* self)
CODE:
{
    dMARIA;

    RETVAL = 0;
    if ( maria->current_state == STATE_DISCONNECTED ) {
        maria->query_results = newSVpvs("Not connected to MySQL, automatically failed ping");
    }
    else if ( maria->current_state != STATE_STANDBY || maria->is_cont ) {
        croak("Cannot ping an active connection!!"); /* TODO moar info */
    }
    else if ( maria->query_sv ) {
        croak("Cannot ping when we have a query queued to be run");
    }
    else {
        maria->current_state = STATE_PING;
        RETVAL = do_work(self, 0);
    }
}
OUTPUT: RETVAL

SV*
ping_result(SV* self)
CODE:
{
    dMARIA;
    if ( maria->is_cont )
        croak("Cannot get the results of the ping, because we are still waiting on the server to respond!");

    RETVAL               = maria->query_results;
    maria->query_results = NULL;
}
OUTPUT: RETVAL

IV
run_query_start(SV* self, SV * query)
CODE:
{
    dMARIA;

    /* TODO would be pretty simple to implement a pipeline here... */
    if ( maria->query_sv )
        croak("Query already waiting to be run!!!!");

    /* See if we can cheat!  We don't need to copy the query's buffer yet --
     * if we do into the state machine and manage to run mysql_real_query_start,
     * we won't need to copy this at all!
     */
    maria->query_sv = query; /* yeah, sharing the SV, for now.  See the comment above */
    SvREFCNT_inc(query);

    if ( maria->is_cont ) {
        if ( maria->current_state == STATE_QUERY ) {
            /*
             * How we get here:
             *  $maria->run_query_start("select 1");
             *  $maria->run_query_start("select 2");
             * This is a no-go, and usually happens when a
             * handle is used from multiple places.
             */
             croak("Cannot start running a second query while we are still completing the first!");
        }

        /*
         * Easy way to get here:
         *      $maria->connect_start(...);
         *      $maria->run_query_start(...);
         * So we started connecting, and immediately queued up
         * a query.  That's fine.
         * However, we really should not go into the state machine,
         * because it will be called with event=0, meaning it will
         * do a poll() on the socket, waiting for the
         * response of the connect, which unless we are connecting
         * to localhost, it's just not going to happen.
         * So avoid all of that and return immediately, and tell
         * our caller to wait on what connect_start returned before.
         */
        RETVAL = maria->last_status;
    }
    else {
        RETVAL = do_work(self, 0);
    }

    if ( maria->query_sv ) {
        /* Lousy.  We did not manage to go far enough into the state machine.
         * We now have to copy the query SV -- keeping the original around is
         * NOT an option since it may be re-used by our caller
         * XXX TODO: could keep the original while replacing query's internals
         * to point to a COW string.  That might lead to less copying.
         */
        SvREFCNT_dec(query); /* since we increased it before */
        maria->query_sv = newSVsv(maria->query_sv);
                         /* ^ the sole refcnt will belong to us */
    }
}
OUTPUT: RETVAL

const char*
current_state(SV* self)
CODE:
{
    dMARIA;
    RETVAL = state_to_name[maria->current_state];
}
OUTPUT: RETVAL

IV
cont(SV* self, ...)
ALIAS:
    run_query_cont = 1
    ping_cont      = 2
    connect_cont   = 3
CODE:
{
    dMARIA;
    IV event = 0;

    if ( !maria->is_cont ) {
        croak("Calling ->%s, but we are not currently waiting for the server. Current state is %s", cont_to_name[ix], state_to_name[maria->current_state]);
    }

    /*
     * If we have more than one item, it should be the event(s) that
     * happened since we were last called -- e.g. the read event on
     * the socket.  If not passed in, the library will poll() on
     * the socket.
     */
    if ( items > 1 ) {
        if ( ST(1) == self )
            croak("Called $self->%s($self), that is just wrong", cont_to_name[ix]);
        event = SvIV(ST(1));
    }

    RETVAL = do_work(self, event);
}
OUTPUT: RETVAL

SV*
query_results(SV* self)
CODE:
    dMARIA;
    RETVAL = newRV_noinc(maria->query_results ? maria->query_results : MUTABLE_SV(newAV())); /* newRV_noinc will take ownership of the refcount */
    maria->query_results = NULL;
OUTPUT: RETVAL

UV
get_timeout_value_ms(SV* self)
CODE:
    dMARIA;
    RETVAL = mysql_get_timeout_value_ms(maria->mysql);
OUTPUT: RETVAL

NV
insert_id(SV* self)
CODE:
    dMARIA;
    RETVAL = mysql_insert_id(maria->mysql);
OUTPUT: RETVAL

SV*
quote(SV* self, SV* to_be_quoted)
CODE:
{
    dMARIA;

    if ( !SvOK(to_be_quoted) ) {
        RETVAL = newSVpvs("NULL");
    }
    else {
    STRLEN to_be_quoted_len;
    const char* to_be_quoted_pv = SvPV(to_be_quoted, to_be_quoted_len);

    if ( !to_be_quoted_len ) {
        RETVAL = newSVpvs("\"\"");
    }
    else {
        UV new_length;
        char * escaped_buffer;
        STRLEN new_len_needed;
        
        if ( (to_be_quoted_len+3) > (MEM_SIZE_MAX/2) ) {
            croak("Cannot quote absurdly long string, would cause an overflow");
        }

        /* mysql_real_escape_string needs len of string*2 plus one byte for the null */
        /* we mimick DBI behavior and return this with quotes */
        new_len_needed = to_be_quoted_len * 2 + 3;

        RETVAL         = newSV(new_len_needed);
        escaped_buffer = SvPVX_mutable(RETVAL);
        escaped_buffer[0] = '\'';

        SvPOK_on(RETVAL);

        new_length = mysql_real_escape_string(
            maria->mysql,
            escaped_buffer+1,
            to_be_quoted_pv,
            to_be_quoted_len
        );
        escaped_buffer[new_length+1] = '\'';
        escaped_buffer[new_length+2] = '\0';
        SvCUR_set(RETVAL, (STRLEN)new_length + 2);
        if ( SvUTF8(to_be_quoted) )
            SvUTF8_on(RETVAL);
    }
    }
}
OUTPUT: RETVAL

void
disconnect(SV* self)
CODE:
    dMARIA;
    disconnect_generic(maria);


