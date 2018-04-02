Cleaner

Requirements:
* Python 3.6
* psycopg2

Usage:
-p BUSY_SPACE_PERCENT, --busy_space_percent BUSY_SPACE_PERCENT
                        Leave X percent busy.

-f MOUNT_TO, --mount_to MOUNT_TO
                        Name of mounting point (for ex. /home, /var/lib).

-b DELAY_BEFORE_REMOVE, --delay_before_remove DELAY_BEFORE_REMOVE
                        Wait before remove file from filesystem and database, in seconds. Default - 15.

-a DELAY_AFTER_REMOVE, --delay_after_remove DELAY_AFTER_REMOVE
                       Wait after remove file, in seconds. Default - 15.

Example of usage:
python3.6 worker.py -p 60
