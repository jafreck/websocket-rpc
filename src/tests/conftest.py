import ssl

import pytest
import trustme

LISTEN_PORT = 1234


@pytest.fixture
def port() -> int:
    global LISTEN_PORT
    LISTEN_PORT += 1
    return LISTEN_PORT


def construct_tls12_restrictive_ssl_context() -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
    context.options |= ssl.OP_NO_TLSv1
    context.options |= ssl.OP_NO_TLSv1_1
    context.options |= ssl.OP_NO_SSLv2
    context.options |= ssl.OP_NO_SSLv3
    return context


@pytest.fixture
def tls_certificate_authority() -> trustme.CA:
    return trustme.CA()


@pytest.fixture
def tls_certificate(tls_certificate_authority: trustme.CA) -> trustme.LeafCert:
    return tls_certificate_authority.issue_server_cert("localhost", "127.0.0.1", "::1")


@pytest.fixture
def server_ssl_ctx(tls_certificate: trustme.LeafCert) -> ssl.SSLContext:
    ssl_ctx = construct_tls12_restrictive_ssl_context()
    tls_certificate.configure_cert(ssl_ctx)
    return ssl_ctx


@pytest.fixture
def client_ssl_ctx(tls_certificate_authority: trustme.CA) -> ssl.SSLContext:
    ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    tls_certificate_authority.configure_trust(ssl_ctx)
    return ssl_ctx
