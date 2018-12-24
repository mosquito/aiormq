from yarl import URL


def censor_url(url: URL):
    if url.password is not None:
        return url.with_password('******')
    return url
