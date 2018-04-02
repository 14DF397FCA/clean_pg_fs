import psycopg2
import argparse
import os
import time
import logging

logger = logging.getLogger("file_system_cleaner")
logger.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    "%(asctime)s [%(filename)s.%(lineno)d] %(processName)s %(levelname)-1s %(name)s - %(message)s")

fh = logging.FileHandler("file_system_cleaner.log")
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)

db_host = "127.0.0.1"
db_port = 5432
db_user = "postgres"
db_pass = ""
db_name = "db-name"


def remove_files(db_resp, conn, delay_before_remove, delay_after_remove):
    logger.debug("Removing files - %s", db_resp)
    if db_resp and conn:
        if len(db_resp) > 0:
            for row in db_resp:
                filename = row[0]  # filename
                logger.debug("Will removed file - %s", str(filename).strip())
                time.sleep(delay_before_remove)
                execute_remove_file(filename=filename)
                time.sleep(delay_before_remove)
                execute_remove_db_row(conn=conn, marker=filename)
                time.sleep(delay_after_remove)


def execute_remove_db_row(marker, conn):
    q = _remove_db_row(marker=marker)
    execute_query(conn=conn, query=q)


def _remove_db_row(marker):
    if len(marker) > 0:
        # q = "DELETE FROM security WHERE event_time_stamp = '" + str(marker) + "';"
        q = "DELETE FROM security WHERE filename = '" + str(marker) + "';"
        logger.debug("Generated query for remove rows from table - %s", q)
        return q


def execute_remove_file(filename):
    filename = str(filename).strip()
    try:
        logger.debug("Trying to remove file " + str(filename))
        os.remove(filename)
    except OSError as e:
        logger.debug("Can't remove file - %s, with error - %s", str(filename), e.strerror)


def get_last_records(conn):
    logger.debug("Get oldest lines in table")
    if conn:
        q = _query_get_last_records()
        result = execute_query(conn=conn, query=q, fetch=True)
        r = []
        for res in result:
            r.append((res[0], res[1]))
        logger.debug("Oldest lines in table - %s", r)
        return r
    else:
        return None


def _query_get_last_records():
    return "SELECT filename,event_time_stamp FROM security ORDER BY event_time_stamp ASC LIMIT 1;"


def open_connect_to_db():
    logger.debug("Trying to connect with database")
    try:
        return psycopg2.connect(database=db_name, user=db_user, password=db_pass, host=db_host, port=db_port)
    except psycopg2.Error as e:
        logger.debug("Can't connect with database - ", e.diag.message_primary)
        exit(999)


def close_connect_to_db(conn):
    try:
        conn.close()
    except psycopg2.Error as e:
        logger.debug("Can't close connection to DB with error", e.diag.message_primary)
    finally:
        conn.close()


def open_cursor(conn):
    try:
        logger.debug("Cursor is open")
        return conn.cursor()
    except psycopg2.Error as e:
        logger.debug("Cursor not opened, error - %s", e.diag.message_primary)
        return None


def execute_query(conn, query, fetch=False):
    if len(query) > 0:
        cursor = open_cursor(conn=conn)
        try:
            cursor.execute(query)
            conn.commit()
            if fetch == True:
                result = cursor.fetchall()
                logger.debug("Query executed success - %s, with result - %s", query, result)
                return result
            else:
                logger.debug("Query executed success - %s, remove rows - %s", query, cursor.rowcount)
        except psycopg2.Error as e:
            logger.debug("Can't execute query '%s' with error - %s", query, e.diag.message_primary)
        finally:
            cursor.close()


def read_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--busy_space_percent", type=int,
                        help="Leave X percent busy.",
                        required=True)
    parser.add_argument("-f", "--mount_to", type=str,
                        help="Name of mounting point (for ex. /home, /var/lib).",
                        required=False, default="/home")
    parser.add_argument("-b", "--delay_before_remove", type=int,
                        help="Wait before remove file from filesystem and database, in seconds. Default - 15.",
                        required=False, default=15)
    parser.add_argument("-a", "--delay_after_remove", type=int,
                        help="Wait after remove file, in seconds. Default - 15.",
                        required=False, default=15)
    args = parser.parse_args()
    return args


def get_used_space_percent(filesystem):
    logger.debug("Get used file space")
    df_output_lines = [s.split() for s in os.popen("df -h ").read().splitlines()]
    for df in df_output_lines[1:]:
        if df[5] == filesystem:
            used = int(df[4][:-1])
            logger.debug("Used file space - %s", used)
            return used


def clear_filesystem(busy_now, busy_after_work, delay_before_remove, delay_after_remove):
    conn = open_connect_to_db()
    while busy_now > busy_after_work:
        logger.debug("Now busy - %s percents, should %s percents. Will removed - %s percent(s).", busy_now,
                     busy_after_work,
                     busy_now - busy_after_work)
        last_records = get_last_records(conn=conn)
        remove_files(conn=conn, db_resp=last_records, delay_before_remove=delay_before_remove,
                     delay_after_remove=delay_after_remove)
    close_connect_to_db(conn=conn)
    logger.debug("Clearing completed")


def main():
    config = read_args()
    busy_after_work = config.busy_space_percent
    mount_to = config.mount_to
    busy_now = get_used_space_percent(mount_to)
    delay_before_remove = config.delay_before_remove
    delay_after_remove = config.delay_after_remove
    logger.debug("Current busy on %s - %s", mount_to, busy_now)
    clear_filesystem(busy_now=busy_now, busy_after_work=busy_after_work,
                     delay_before_remove=delay_before_remove,
                     delay_after_remove=delay_after_remove)


if __name__ == '__main__':
    main()
