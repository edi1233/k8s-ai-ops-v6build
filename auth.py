"""Portal authentication.

Password protection has been intentionally DISABLED per operator request:
`require_auth` is a no-op dependency that always allows the request through,
and it declares no HTTPBasic dependency, so browsers get no login prompt.

To re-enable auth later, restore an HTTPBasic-based dependency here.
"""


def require_auth():
    # Auth disabled: allow all requests, never issue a 401 challenge.
    return None
