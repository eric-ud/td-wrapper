import teradatasql
import os
import subprocess


class TeradataConnectionFromKeyring(teradatasql.TeradataConnection):
    def __init__(self, **kwargs):
        HOST = os.environ.get("teradata_host")
        USERNAME = os.environ.get("teradata_username")
        KEYRING_NAME = "teradata"
        PASSWORD = (
            subprocess.run(
                [
                    "/usr/bin/python",
                    "-m",
                    "keyring",
                    "-b",
                    "keyrings.alt.file.PlaintextKeyring",
                    "get",
                    KEYRING_NAME,
                    USERNAME,
                ],
                capture_output=True,
            )
            .stdout.decode("utf-8")
            .strip()
        )

        my_params = {
            "host": HOST,
            "user": USERNAME,
            "password": PASSWORD,
            "tmode": "ANSI",
            "logmech": "LDAP",
        }

        for k, v in my_params.items():
            if kwargs.get(k) is None:
                kwargs[k] = v

        super().__init__(**kwargs)
