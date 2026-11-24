# Upload app-data.json to the public GitHub repo (7788peter/dividend-butler-data).
# Usage: python publish.py C:\dev\dividend-butler\app-data.json
# The token is read from the file %USERPROFILE%\.dbutler-gh-token (or env DBUTLER_TOKEN). It is never printed.
import base64, hashlib, json, os, sys, time, urllib.error, urllib.request

OWNER = "7788peter"
REPO = "dividend-butler-data"
PATH = "app-data.json"
BRANCH = "main"
API = os.environ.get("DBUTLER_API", "https://api.github.com")
MIN_SECURITIES = 1000


def log(msg):
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)


def read_token():
    t = os.environ.get("DBUTLER_TOKEN", "").strip()
    if t:
        return t
    p = os.path.join(os.path.expanduser("~"), ".dbutler-gh-token")
    try:
        with open(p, encoding="utf-8-sig") as f:
            return f.read().strip()
    except OSError:
        return ""


def call(method, url, token, body=None):
    data = None if body is None else json.dumps(body).encode("ascii")
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Authorization", "Bearer " + token)
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("X-GitHub-Api-Version", "2022-11-28")
    req.add_header("User-Agent", "dividend-butler-publish")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=60) as resp:
        return resp.status, json.loads(resp.read().decode("utf-8"))


def git_blob_sha(raw):
    return hashlib.sha1(b"blob %d\0" % len(raw) + raw).hexdigest()


def validate(raw):
    d = json.loads(raw.decode("utf-8"))
    meta, secs = d.get("meta"), d.get("securities")
    if not isinstance(meta, dict) or not isinstance(secs, list):
        raise ValueError("bad structure")
    if not isinstance(meta.get("exportedAt"), str):
        raise ValueError("missing exportedAt")
    if len(secs) < MIN_SECURITIES:
        raise ValueError("only %d securities, expected at least %d" % (len(secs), MIN_SECURITIES))
    return meta["exportedAt"], len(secs)


def main():
    if len(sys.argv) != 2:
        log("ERROR usage: python publish.py <path to app-data.json>")
        return 2
    try:
        with open(sys.argv[1], "rb") as f:
            raw = f.read()
        exported_at, count = validate(raw)
    except (OSError, ValueError) as e:
        log("ERROR local file is not usable, not uploading: %s" % e)
        return 2
    token = read_token()
    if not token:
        log("ERROR no token found (file .dbutler-gh-token in your user folder)")
        return 2
    url = "%s/repos/%s/%s/contents/%s" % (API, OWNER, REPO, PATH)
    local_sha = git_blob_sha(raw)
    for attempt in (1, 2):
        try:
            sha = None
            try:
                _, cur = call("GET", url + "?ref=" + BRANCH, token)
                sha = cur.get("sha")
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise
            if sha == local_sha:
                log("skipped: remote file is already identical (exportedAt %s, %d securities)" % (exported_at, count))
                return 0
            body = {
                "message": "Update app-data.json (exportedAt %s)" % exported_at,
                "content": base64.b64encode(raw).decode("ascii"),
                "branch": BRANCH,
            }
            if sha:
                body["sha"] = sha
            status, res = call("PUT", url, token, body)
            log("uploaded: HTTP %d, exportedAt %s, %d securities, %d bytes" % (status, exported_at, count, len(raw)))
            return 0
        except urllib.error.HTTPError as e:
            detail = ""
            try:
                detail = json.loads(e.read().decode("utf-8")).get("message", "")
            except Exception:
                pass
            if e.code == 409 and attempt == 1:
                log("conflict, retrying once")
                continue
            hint = " (token expired or wrong?)" if e.code in (401, 403) else ""
            log("ERROR HTTP %d %s%s" % (e.code, detail, hint))
            return 1
        except Exception as e:
            log("ERROR %s: %s" % (type(e).__name__, e))
            return 1
    return 1


if __name__ == "__main__":
    sys.exit(main())