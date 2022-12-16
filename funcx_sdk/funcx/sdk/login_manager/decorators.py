import functools


def requires_login(*resource_servers: str):
    """Decorator for specifying resource servers that
    the user must have valid tokens for. If a token is
    found to be invalid, a new login flow is initiated.
    """

    def inner(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            manager = self.login_manager
            for server in resource_servers:
                if not manager.has_login(server):
                    manager.run_login_flow()
            return func(self, *args, **kwargs)

        return wrapper

    return inner
