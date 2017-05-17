#define PERL_NO_GET_CONTEXT 1
#include "EXTERN.h"
#include "perl.h"
#include "XSUB.h"

#include <mysql.h>

#define prepare_new_result_accumulator(maria) STMT_START { \
    maria->query_results ? SvREFCNT_dec(maria->query_results) : NULL; \
    maria->query_results = newAV(); \
} STMT_END
    
typedef struct MariaDB_client {
    MYSQL mysql;
    MYSQL_RES* res;

    AV *query_results;
    SV* query_sv;

    int mysql_socket_fd;
    int current_state;
    int last_status;

    bool is_cont;   /* next operation msut be a _cont */
    bool destroyed; /* DESTROY has been called */
} MariaDB_client;

enum {
    STATE_STANDBY,
    STATE_CONNECT,
    STATE_QUERY_RUN,
    STATE_ROW_FETCH,
    STATE_FREE_RESULT
};

static int
free_mariadb_handle(pTHX_ SV* sv, MAGIC *mg)
{
    MariaDB_client* maria = (MariaDB_client*)mg->mg_ptr;
    if ( !maria->destroyed ) {
        maria->destroyed = 1;
        mysql_close(&(maria->mysql));
        Safefree(maria); /* free the memory we Newxz'd before */
        /* mg will be freed by our caller */
    }

    return 0;
}

static MGVTBL session_magic_vtbl = { .svt_free = free_mariadb_handle };

int
THX_add_row_to_results(pTHX_ MariaDB_client *maria, MYSQL_ROW row)
#define add_row_to_results(a,b) THX_add_row_to_results(aTHX_ a,b)
{
    AV *query_results;
	AV *row_results;
	int field_count;
    unsigned long *lengths;
    int i = 0;

    query_results = maria->query_results;

	field_count   = mysql_field_count(&(maria->mysql));
	lengths       = mysql_fetch_lengths(maria->res);

	row_results   = newAV();

    for ( i = 0; i < field_count; i++ ) {
		SV *col_data = row[i]
		                ? newSVpvn(row[i], (STRLEN)lengths[i])
		                : &PL_sv_undef;
		av_push(row_results, col_data);
	}
	av_push(query_results, newRV_noinc(MUTABLE_SV(row_results)));

    return 1;
}

int
THX_do_stuff(pTHX_ MariaDB_client *maria, IV event)
#define do_stuff(maria, event) THX_do_stuff(aTHX_ maria, event)
{
    int err        = 0;
    int status     = 0;
    int state      = maria->current_state;
    const char* errstring;

    /* TODO save status */
    do {
        switch ( state ) {
            case STATE_STANDBY:
                if ( maria->query_sv ) {
                    /* we have a query sv saved, so go ahead and run our query! */
                    state = STATE_QUERY_RUN;
                }
                /* Otherwise, the loop will just end */
                break;
            case STATE_CONNECT:
                if ( maria->is_cont ) {
                    
                }
                else {
                    
                }

                err = 1;
                errstring = "Reconnecting / Async connections are not yet implemented";

                if ( !err && !status ) {
                    /* connected successfully! */
                    if ( maria->query_sv ) {
                        /* need to run a query next */
                        state = STATE_QUERY_RUN;
                    }
                    else {
                        state = STATE_STANDBY;
                    }
                }
                break;
            case STATE_QUERY_RUN:
                if ( maria->is_cont ) {
                    status = mysql_real_query_cont(&err, &(maria->mysql), event);
                }
                else {
                    SV* query_sv   = maria->query_sv;
                    STRLEN query_len;
                    char* query_pv = SvPV(query_sv, query_len);
                    status = mysql_real_query_start(
                                &err,
                                &(maria->mysql),
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

                    errstring = mysql_error(&(maria->mysql));
                }
                else {
                    if ( status ) {
                        maria->is_cont = TRUE; /* need to wait on the socket */
                    }
                    else {
                        maria->is_cont = FALSE; /* hooray! */
                        maria->res     = mysql_use_result(&(maria->mysql));

                        if ( !maria->res ) {
                            croak("mysql_use_result failed, wtf?");
                        }

                        state = STATE_ROW_FETCH;
                    }
                }
                break;
            case STATE_FREE_RESULT:
                if ( maria->is_cont ) {
                    status = mysql_free_result_cont(maria->res, event);
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
                    if (!status) {
                        if ( row ) {
                            add_row_to_results(maria, row);
                        }
                        else {
                            if ( mysql_errno(&(maria->mysql)) > 0 ) {
                                err       = 1;
                                errstring = mysql_error(&(maria->mysql));
                            }
                            state = STATE_FREE_RESULT;
                        }
                        maria->is_cont = FALSE;
                    }
                }
                else {
                    do {
                        status = mysql_fetch_row_start(&row, maria->res);
                        if (!status) {
                            if ( row ) {
                                add_row_to_results(maria, row);
                            }
                            else if ( mysql_errno(&(maria->mysql)) > 0 ) {
                                /* Damn... We got an error while fetching
                                 * the rows.  Need to free the resultset
                                 */
                                err       = 1;
                                errstring = mysql_error(&(maria->mysql));
                                state     = STATE_FREE_RESULT;
                            }
                        }
                    } while (!status && row);

                    if ( status ) {
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
        } /* end of switch (state) */
    } while ( state != STATE_STANDBY && !maria->is_cont );

    /*
     * We croak outside of the while, to give it a chance
     * to free the result
     */
    if ( err ) {
        maria->is_cont       = FALSE;
        maria->current_state = STATE_STANDBY;
        maria->last_status   = 0;
        if (!errstring)
            errstring = "Unknown MySQL error";
        croak("%s", errstring);
    }

    maria->current_state = state;
    maria->last_status   = status;

    return status;
}

void
THX_run_blocking_query(pTHX_ MariaDB_client* maria, SV* query_sv, bool want_results)
#define run_blocking_query(x,y,z) THX_run_blocking_query(aTHX_ x,y,z)
{
    int err = 0;
    STRLEN query_len;
    const char* query_pv = SvPV(query_sv, query_len);

    /* TODO check for state == standby */

    err = mysql_real_query( &(maria->mysql), query_pv, query_len );

    if ( err ) {
        croak("welp!"); /* mysql_errstring */
    }

    maria->res = mysql_store_result(&(maria->mysql));

    prepare_new_result_accumulator(maria);

    if ( maria->res && want_results ) {
        MYSQL_ROW row;
        while ((row = mysql_fetch_row(maria->res)))
            add_row_to_results(maria, row);
    }

    mysql_free_result(maria->res);
    maria->res = NULL;
}

MariaDB_client*
mariadb_init(void)
{
    MariaDB_client *maria;
    Newxz(maria, 1, MariaDB_client);

    maria->query_results = NULL;
    maria->current_state = STATE_STANDBY;
    maria->mysql_socket_fd = -1;

    mysql_init(&(maria->mysql));
    mysql_options(&(maria->mysql), MYSQL_OPT_NONBLOCK, 0);

    return maria;
}

char*
THX_easy_arg_fetch(pTHX_ HV *hv, char * pv, STRLEN pv_len)
#define easy_arg_fetch(hv, s) THX_easy_arg_fetch(aTHX_ hv, s, sizeof(s)-1)
{
    SV** svp;
    char *res = NULL;

    if ((svp = hv_fetch(hv, pv, pv_len, FALSE))) {
        if ( SvOK(*svp) ) {
            STRLEN len;
            res = SvPV(*svp, len);
        }
        /* it can stay NULL for undef */
    }
    else {
        croak("No %s given to ->connect!", pv);
    }
    return res;
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

MariaDB_client*
connect(char* CLASS, HV* args)
CODE:
{
    MYSQL *connect_return;
    MariaDB_client *maria = mariadb_init();

    char* host         = easy_arg_fetch(args, "host");
    char* port_str     = easy_arg_fetch(args, "port");
    char* user         = easy_arg_fetch(args, "user");
    char* password     = easy_arg_fetch(args, "password");
    char* database     = easy_arg_fetch(args, "database");
    char* mysql_socket = easy_arg_fetch(args, "mysql_socket");
    unsigned int port  = 0;

    if ( port_str )
        port = atoi(port_str); /* eh */

    connect_return = mysql_real_connect(
                        &(maria->mysql),
                        host,
                        user,
                        password,
                        database,
                        port,
                        mysql_socket,
                        0 /* not exposing this yet */
                    );

    if ( !connect_return )
        croak("Failed to connect to MySQL: %s\n", mysql_error(&(maria->mysql)));

    maria->mysql_socket_fd = mysql_get_socket(&(maria->mysql));

    /* TODO get thread id */

    RETVAL = maria;
}
OUTPUT: RETVAL

int
mysql_socket_fd(MariaDB_client* maria)
CODE:
    RETVAL = maria->mysql_socket_fd;
OUTPUT: RETVAL

IV
run_query_start(MariaDB_client* maria, SV * query)
CODE:
{
    if ( maria->current_state != STATE_STANDBY && maria->current_state != STATE_CONNECT )
        croak("Cannot call run_query_start because we are in the middle of a query");

    maria->query_sv = query; /* yeah, sharing the SV */
    SvREFCNT_inc(query);

    prepare_new_result_accumulator(maria);
    RETVAL = do_stuff(maria, 0);
}
OUTPUT: RETVAL

IV
run_query_cont(MariaDB_client* maria, ...)
CODE:
{
    IV event = 0;
    if ( !maria->is_cont )
        croak("Calling run_query_cont, but the internal state does not match up to that");
    if ( items > 1 )
        event = SvIV(ST(1));
    RETVAL = do_stuff(maria, event);
}
OUTPUT: RETVAL

SV*
query_results(MariaDB_client* maria)
CODE:
    RETVAL = newRV(MUTABLE_SV(maria->query_results));
OUTPUT: RETVAL

UV
get_timeout_value_ms(MariaDB_client* maria)
CODE:
    RETVAL = mysql_get_timeout_value_ms(&(maria->mysql));
OUTPUT: RETVAL

void
disconnect(MariaDB_client* maria)
CODE:
    if ( maria->query_sv ) {
        SvREFCNT_dec(maria->query_sv);
        maria->query_sv = NULL;
    }
    if ( maria->res ) {
        /* do a best attempt... */
        int status = mysql_free_result_start(maria->res);
        if ( status ) {
            /* Damn.  Would've blocked trying to free the result.  Not much we can do */
        }
        maria->res     = NULL;
    }
    mysql_close(&(maria->mysql));
    maria->is_cont       = FALSE;
    maria->current_state = STATE_CONNECT;



SV*
selectall_arrayref(MariaDB_client* maria, SV* query_sv)
CODE:
{
    run_blocking_query(maria, query_sv, TRUE);
    RETVAL = newRV(MUTABLE_SV(maria->query_results));
}
OUTPUT: RETVAL


IV
do(MariaDB_client* maria, SV* query_sv)
CODE:
{
    run_blocking_query(maria, query_sv, FALSE);
    /* TODO get num_rows and friends */
    RETVAL = -1;
}
OUTPUT: RETVAL

int
real_query_start(MariaDB_client* maria, SV * query)
CODE:
{
    int err;
    STRLEN query_len;
    char * query_pv = SvPV(query, query_len);

    prepare_new_result_accumulator(maria);

    RETVAL = mysql_real_query_start(&err, &(maria->mysql), query_pv, query_len);
    if ( !RETVAL ) {
        maria->res = mysql_use_result(&(maria->mysql));
        if ( !maria->res ) {
            croak("mysql_use_result failed, wtf?");
        }
    }
}
OUTPUT: RETVAL

int
real_query_cont(MariaDB_client* maria)
CODE:
{
    int err;
    RETVAL = mysql_real_query_cont(&err, &(maria->mysql), 0);
    if ( !RETVAL ) {
        maria->res = mysql_use_result(&(maria->mysql));
        if ( !maria->res ) {
            croak("mysql_use_result failed, wtf?");
        }
    }
}
OUTPUT: RETVAL

int
fetch_row_start(MariaDB_client* maria)
CODE:
{
    MYSQL_ROW row;
    RETVAL = mysql_fetch_row_start(&row, maria->res);
    if (!RETVAL) {
        RETVAL = add_row_to_results(maria, row);
    }
    else {
        maria->is_cont = TRUE;
    }
}
OUTPUT: RETVAL

int
fetch_rows_start(MariaDB_client* maria)
CODE:
{
    int status = 0;
    MYSQL_ROW row = 0;
    do {
        status = mysql_fetch_row_start(&row, maria->res);
        if ( !status ) {
            (void)add_row_to_results(maria, row);
        }
    }
    while (row && status == 0);
    RETVAL = status;
}
OUTPUT: RETVAL

bool
have_more_results(MariaDB_client* maria)
CODE:
    RETVAL = cBOOL(maria->res);
OUTPUT: RETVAL



int
fetch_row_cont(MariaDB_client* maria)
CODE:
{
    MYSQL_ROW row;
    RETVAL = mysql_fetch_row_cont(&row, maria->res, 0);
    if (!RETVAL) {
        maria->is_cont = FALSE;
        RETVAL = add_row_to_results(maria, row);
    }
}
OUTPUT: RETVAL

