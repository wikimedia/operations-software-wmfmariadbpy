#!/usr/bin/python3

import os
import sys

import wmfmariadbpy.dbutil as dbutil

"""
mysql.py intends to be a wrapper around the mysql command line client,
adapted to WMF cluster convenience -intended for its usage on command
line/bash scripts only. It allows to skip full domain name
and auto-completes it in a best-effor manner. It also forces the usage
of TLS, unless socket based authentication is used (localhost).
It allow to define hosts in host:port format.
Finally, it handles the several domains by chosing the right default
password (e.g. labsdb hosts have its own, separate password.
Other than automatically add some extra parameters, the script just
execs mysql, behaving like it.
"""


def find_host(arguments):
    """
    Determines the host, if any, provided on command line and
    and index on where that host is defined. Trickier than it
    should as mysql accepts --host=host, -hhost, -h host and
    --host host.
    """
    i = 0
    host = None
    host_index = []
    for argument in arguments:
        if argument.startswith("-h"):
            if len(argument[2:]) > 0:
                host = argument[2:]
                host_index.append(i)
            elif argument == "-h" and len(arguments) > (i + 1):
                host = arguments[i + 1]
                host_index.append(i)
                host_index.append(i + 1)
        elif argument.startswith("--host"):
            if argument[6:7] == "=":
                host = argument[7:]
                host_index.append(i)
            elif argument == "--host" and len(arguments) > (i + 1):
                host = arguments[i + 1]
                host_index.append(i)
                host_index.append(i + 1)
        i += 1
    return (host, host_index)


def override_arguments(arguments):
    """
    Finds the host parameters and applies ssl config, host/port
    transformations and default section (password) used
    """
    host, host_index = find_host(arguments)
    if host is None:
        arguments.append("--skip-ssl")
        return arguments
    host, port = dbutil.resolve(host)

    # Just add skip-ssl for localhost
    if host == "localhost":
        arguments.append("--skip-ssl")

    # Add complete host and port
    for i in host_index:
        del arguments[host_index[0]]
    arguments.insert(host_index[0], "--host={}".format(host))
    if port is not None:
        arguments.insert(host_index[0] + 1, "--port={}".format(port))

    # different auth for labsdb hosts
    if host.startswith("labsdb") or host.startswith("clouddb"):
        arguments.insert(1, "--defaults-group-suffix=labsdb")

    return arguments


def main():
    arguments = override_arguments(sys.argv)
    sys.exit(os.execvp("mysql", arguments))


if __name__ == "__main__":
    main()
