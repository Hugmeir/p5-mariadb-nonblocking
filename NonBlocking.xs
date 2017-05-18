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

#define dMARIA MariaDB_client* maria; STMT_START { \
    {\
        SV *self = ST(0);\
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
#define NAMES C(STANDBY)C(CONNECT)C(QUERY)C(ROW_FETCH)C(FREE_RESULT)
#define C(x) STATE_##x,
enum color { NAMES STATE_END };
#undef C
#define C(x) #x,
const char * const state_to_name[] = { NAMES };
#undef C

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

static MGVTBL maria_vtable = { .svt_free = free_mariadb_handle };

int
THX_add_row_to_results(pTHX_ MariaDB_client *maria, MYSQL_ROW row)
#define add_row_to_results(a,b) THX_add_row_to_results(aTHX_ a,b)
{
    AV *query_results;
    AV *row_results;
    int field_count;
    unsigned long *lengths;
    SSize_t i = 0;

    query_results = maria->query_results;

    field_count   = mysql_field_count(&(maria->mysql));
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
                    /* we have a query sv saved, so go ahead and run it! */
                    state = STATE_QUERY;
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
                        state = STATE_QUERY;
                    }
                    else {
                        state = STATE_STANDBY;
                    }
                }
                break;
            case STATE_QUERY:
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
                else if ( status ) {
                    maria->is_cont = TRUE; /* need to wait on the socket */
                }
                else {
                    /* query finished */
                    maria->is_cont = FALSE; /* hooray! */
                    maria->res     = mysql_use_result(&(maria->mysql));

                    if ( maria->res ) {
                        /* query returned and we have results,
                         * start fetching!
                         */
                        state = STATE_ROW_FETCH;
                    }
                    else if ( mysql_field_count(&(maria->mysql)) == 0 ) {
                        /* query succeeded but returned no data */
                        /* TODO might want to store affected rows? */
                        /*rows = mysql_affected_rows(&(maria->mysql));*/
                        prepare_new_result_accumulator(maria); /* empty results */
                        state = STATE_STANDBY;
                    }
                    else {
                        /* Error! */
                        err       = 1;
                        errstring = mysql_error(&(maria->mysql));
                        /* maria->res is NULL if we get here, so no need
                         * to go and free that
                         */
                        state  = STATE_STANDBY;
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

IV
THX_run_blocking_query(pTHX_ MariaDB_client* maria, SV* query_sv, bool want_results)
#define run_blocking_query(x,y,z) THX_run_blocking_query(aTHX_ x,y,z)
{
    IV rows = -1;
    /* ^ default return value: we have no clue what the rows affected was */
    int err = 0;
    STRLEN query_len;
    const char* query_pv = SvPV(query_sv, query_len);

    if ( maria->current_state != STATE_STANDBY )
        croak("Cannot run a blocking query on this handle, because the internal state is on %s", state_to_name[maria->current_state]);

    err = mysql_real_query( &(maria->mysql), query_pv, query_len );

    if ( err ) {
        croak("welp!"); /* mysql_errstring */
    }

    maria->res = mysql_store_result(&(maria->mysql));

    prepare_new_result_accumulator(maria);

    if ( maria->res ) {
        if ( want_results ) {
            MYSQL_ROW row;
            while ((row = mysql_fetch_row(maria->res)))
                add_row_to_results(maria, row);
        }
        rows = mysql_affected_rows(&(maria->mysql));
        mysql_free_result(maria->res);
        maria->res = NULL;
    }
    else if ( mysql_field_count(&(maria->mysql)) == 0 ) {
        /* query succeeded but returned no data */
        rows = mysql_affected_rows(&(maria->mysql));
    }
    else {
        /* Error! */
        croak("%s", mysql_error(&(maria->mysql)));
    }

    return rows;
}

SV*
THX_init_self(pTHX_ SV* classname)
#define init_self(c) THX_init_self(aTHX_ c)
{
    SV* self;

    MariaDB_client *maria;
    Newxz(maria, 1, MariaDB_client);

    maria->query_results = NULL;
    maria->current_state = STATE_STANDBY;
    maria->mysql_socket_fd = -1;

    mysql_init(&(maria->mysql));
    mysql_options(&(maria->mysql), MYSQL_OPT_NONBLOCK, 0);

    /* create a reference to a hash, bless into CLASS, then add the
     * magic we need
     */
    self = newRV_noinc(MUTABLE_SV(newHV()));
    sv_bless(self, gv_stashsv(classname, GV_ADD));
    sv_magicext(
        SvRV(self),
        SvRV(self),
        PERL_MAGIC_ext,
        &maria_vtable,
        (char*) maria,
        0
    );

    return self;
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

SV*
init(SV* classname)
CODE:
RETVAL = init_self(classname);
OUTPUT: RETVAL

SV*
connect(SV* class_or_self, HV* args)
CODE:
{
    SV *self;
    MariaDB_client* this_maria;
    MYSQL *connect_return;

    MAGIC *mg;
    if (
           sv_isobject(class_or_self) && SvTYPE(SvRV(class_or_self)) == SVt_PVHV
        && mg_findext(SvRV(class_or_self), PERL_MAGIC_ext, &maria_vtable) != 0
    ) {
        dMARIA;
        self = class_or_self;
        this_maria = maria;
    }
    else {
        MAGIC* mg;
        self = init_self(class_or_self);
        mg = mg_findext(SvRV(self), PERL_MAGIC_ext, &maria_vtable);
        this_maria = (MariaDB_client*) mg->mg_ptr;
    }

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
                        &(this_maria->mysql),
                        host,
                        user,
                        password,
                        database,
                        port,
                        mysql_socket,
                        0 /* not exposing this yet */
                    );

    if ( !connect_return )
        croak("Failed to connect to MySQL: %s\n", mysql_error(&(this_maria->mysql)));

    this_maria->mysql_socket_fd = mysql_get_socket(&(this_maria->mysql));

    /* TODO get thread id */
    hv_stores(
        MUTABLE_HV(SvRV(self)),
        "mysql_socket_fd",
        newSViv(this_maria->mysql_socket_fd)
    );

    RETVAL = self;
}
OUTPUT: RETVAL

int
mysql_socket_fd(SV* self)
CODE:
    dMARIA;
    RETVAL = maria->mysql_socket_fd;
OUTPUT: RETVAL

IV
run_query_start(SV* self, SV * query)
CODE:
{
    dMARIA;
    if ( maria->current_state != STATE_STANDBY )
        croak("Cannot call run_query_start because the current internal state says we are on %s", state_to_name[maria->current_state]);

    maria->query_sv = query; /* yeah, sharing the SV */
    SvREFCNT_inc(query);

    prepare_new_result_accumulator(maria);
    RETVAL = do_stuff(maria, 0);
}
OUTPUT: RETVAL

IV
run_query_cont(SV* self, ...)
CODE:
{
    dMARIA;
    IV event = 0;
    if ( !maria->is_cont )
        croak("Calling run_query_cont, but the internal state does not match up to that");
    if ( items > 1 )
        event = SvIV(ST(1));
    RETVAL = do_stuff(maria, event);
}
OUTPUT: RETVAL

SV*
query_results(SV* self)
CODE:
    dMARIA;
    RETVAL = newRV(MUTABLE_SV(maria->query_results));
OUTPUT: RETVAL

UV
get_timeout_value_ms(SV* self)
CODE:
    dMARIA;
    RETVAL = mysql_get_timeout_value_ms(&(maria->mysql));
OUTPUT: RETVAL

void
disconnect(SV* self)
CODE:
    dMARIA;
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
selectall_arrayref(SV* self, SV* query_sv)
CODE:
{
    dMARIA;
    (void)run_blocking_query(maria, query_sv, TRUE);
    RETVAL = newRV(MUTABLE_SV(maria->query_results));
}
OUTPUT: RETVAL


IV
do(SV* self, SV* query_sv)
CODE:
{
    dMARIA;
    RETVAL = run_blocking_query(maria, query_sv, FALSE);
}
OUTPUT: RETVAL

int
real_query_start(SV* self, SV * query)
CODE:
{
    dMARIA;
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
real_query_cont(SV* self)
CODE:
{
    dMARIA;
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
fetch_row_start(SV* self)
CODE:
{
    dMARIA;
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
fetch_rows_start(SV* self)
CODE:
{
    dMARIA;
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
have_more_results(SV* self)
CODE:
    dMARIA;
    RETVAL = cBOOL(maria->res);
OUTPUT: RETVAL



int
fetch_row_cont(SV* self)
CODE:
{
    dMARIA;
    MYSQL_ROW row;
    RETVAL = mysql_fetch_row_cont(&row, maria->res, 0);
    if (!RETVAL) {
        maria->is_cont = FALSE;
        RETVAL = add_row_to_results(maria, row);
    }
}
OUTPUT: RETVAL

