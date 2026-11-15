import base64, hmac, hashlib, json, time
def b64(b): return base64.urlsafe_b64encode(b).rstrip(b'=').decode()
def token(sub, role='authenticated', iat=None, exp=None, extra=None):
    secret = open('/workspace/coalesce/secrets/jwt_secret').read().strip().encode()
    iat = iat or 1763200000
    c = {'sub': str(sub), 'role': role, 'iat': iat, 'exp': exp or iat + 3600 * 24 * 400}
    c.update(extra or {})
    h = b64(json.dumps({'alg': 'HS256', 'typ': 'JWT'}, separators=(',', ':')).encode())
    p = b64(json.dumps(c, separators=(',', ':')).encode())
    s = b64(hmac.new(secret, (h + '.' + p).encode(), hashlib.sha256).digest())
    return h + '.' + p + '.' + s
if __name__ == '__main__':
    import sys; print(token(sys.argv[1]))
