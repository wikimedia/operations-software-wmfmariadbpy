"""Utils for testing wmfmariadbpy."""

import datetime
import os
import sys
from typing import Any, Dict, List, Tuple, Union, cast

import pymysql


class hide_stderr:
    """Class used to hide the stderr."""

    class FakeStderr:
        """Class used as a fake stderr."""

        def write(self, s):
            """Just do nothing."""
            pass

    def __enter__(self):
        """Store the real stderr and place the fake one."""
        self.real_stderr = sys.stderr
        sys.stderr = self.FakeStderr()

    def __exit__(self, type, value, traceback):
        """Restore the real stderr."""
        sys.stderr = self.real_stderr


def query_db(port: int, query: str) -> Union[Tuple[()], List[Dict[str, Any]]]:
    print(
        "%s Querying localhost:%d: %s"
        % (datetime.datetime.now().isoformat(), port, query)
    )
    mycnf = os.path.join(os.path.dirname(__file__), "integration_env", "my.cnf")
    conn = pymysql.connect(host="localhost", port=port, read_default_file=mycnf)
    cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
    cur.execute(query)
    # Cursor.fetchall() has a very generic return type annotation as it doesn't know
    # which type of cursor has been instantiated.
    return cast(Union[Tuple[()], List[Dict[str, Any]]], cur.fetchall())
