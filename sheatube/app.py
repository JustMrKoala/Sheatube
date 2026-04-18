import os
import json
import uuid
import mimetypes
import subprocess
import socket
from datetime import datetime
from flask import Flask, request, jsonify, send_file, send_from_directory, abort, session, redirect
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__, static_folder="static")
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret-key-change-me")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")
CHUNK_UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads_tmp")
DATA_FILE = os.path.join(BASE_DIR, "videos.json")
USERS_FILE = os.path.join(BASE_DIR, "users.json")
ALLOWED_EXTENSIONS = {"mp4", "webm", "ogg", "mov", "avi", "mkv"}
ALLOWED_AVATAR_EXTENSIONS = {"png", "jpg", "jpeg", "webp", "gif"}
MAX_AVATAR_FILE_SIZE = 5 * 1024 * 1024  # 5MB
MAX_CONTENT_LENGTH = 500 * 1024 * 1024  # 500MB
CHUNK_SIZE = 8 * 1024 * 1024  # 8MB
SPARK_MAX_DURATION_SECONDS = 60.0
SPARK_MAX_WIDTH = 1080
SPARK_MAX_HEIGHT = 1920
SPARK_MIN_ASPECT_RATIO = 1.3  # portrait-oriented "phone-like" minimum
SPARK_TARGET_WIDTH = 1080
SPARK_TARGET_HEIGHT = 1920
SPARK_TARGET_DURATION_SECONDS = 59.5

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.config["MAX_CONTENT_LENGTH"] = MAX_CONTENT_LENGTH
AVATAR_UPLOAD_FOLDER = os.path.join(app.static_folder, "avatars")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(CHUNK_UPLOAD_FOLDER, exist_ok=True)
os.makedirs(AVATAR_UPLOAD_FOLDER, exist_ok=True)


# ── Data helpers ─────────────────────────────────────────────────────────────

def load_json(path):
    if not os.path.exists(path):
        return []
    with open(path) as f:
        return json.load(f)

def save_json(path, data):
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

def load_videos(): return load_json(DATA_FILE)
def save_videos(v): save_json(DATA_FILE, v)
def load_users():
    users = load_json(USERS_FILE)
    changed = False
    for u in users:
        if "avatar_url" not in u:
            u["avatar_url"] = ""
            changed = True
        if not isinstance(u.get("following"), list):
            u["following"] = []
            changed = True
    if changed:
        save_json(USERS_FILE, users)
    return users
def save_users(u): save_json(USERS_FILE, u)

def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def allowed_avatar_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_AVATAR_EXTENSIONS


def upload_session_path(upload_id):
    return os.path.join(CHUNK_UPLOAD_FOLDER, upload_id)


def upload_meta_path(upload_id):
    return os.path.join(upload_session_path(upload_id), "meta.json")


def load_upload_meta(upload_id):
    path = upload_meta_path(upload_id)
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def save_upload_meta(upload_id, meta):
    with open(upload_meta_path(upload_id), "w") as f:
        json.dump(meta, f, indent=2)


def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    users = load_users()
    return next((u for u in users if u["id"] == uid), None)


def user_public(user, viewer=None):
    users = load_users()
    subscribers_count = sum(1 for u in users if user["id"] in (u.get("following") or []))
    following = user.get("following") or []
    is_subscribed = bool(viewer and user["id"] in (viewer.get("following") or []))
    return {
        "id": user["id"],
        "username": user["username"],
        "joined_at": user["joined_at"],
        "avatar_url": user.get("avatar_url", ""),
        "subscribers_count": subscribers_count,
        "following_count": len(following),
        "is_subscribed": is_subscribed,
    }


def normalize_kind(kind):
    if str(kind or "").strip().lower() == "spark":
        return "spark"
    return "video"


def parse_video_meta(payload):
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except json.JSONDecodeError:
            return None
    payload = payload or {}
    try:
        duration = float(payload.get("duration"))
        width = int(payload.get("width"))
        height = int(payload.get("height"))
    except (TypeError, ValueError):
        return None
    if duration <= 0 or width <= 0 or height <= 0:
        return None
    return {"duration": duration, "width": width, "height": height}


def ffprobe_video(path):
    try:
        result = subprocess.run(
            [
                "ffprobe",
                "-v",
                "error",
                "-print_format",
                "json",
                "-show_entries",
                "format=duration:stream=width,height",
                "-select_streams",
                "v:0",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=12,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return None

    if result.returncode != 0 or not result.stdout:
        return None

    try:
        payload = json.loads(result.stdout)
        stream = (payload.get("streams") or [{}])[0]
        fmt = payload.get("format") or {}
        duration = float(fmt.get("duration"))
        width = int(stream.get("width"))
        height = int(stream.get("height"))
    except (TypeError, ValueError, json.JSONDecodeError, IndexError):
        return None

    if duration <= 0 or width <= 0 or height <= 0:
        return None
    return {"duration": duration, "width": width, "height": height}


def validate_spark_meta(meta):
    if not meta:
        return "Could not read video metadata. Re-encode and try again."
    if meta["duration"] >= SPARK_MAX_DURATION_SECONDS:
        return "Sparks must be shorter than 60 seconds."
    if meta["height"] <= meta["width"]:
        return "Sparks must be portrait (phone-style) videos."
    if meta["width"] > SPARK_MAX_WIDTH or meta["height"] > SPARK_MAX_HEIGHT:
        return f"Sparks max resolution is {SPARK_MAX_WIDTH}x{SPARK_MAX_HEIGHT}."
    aspect = meta["height"] / meta["width"]
    if aspect < SPARK_MIN_ASPECT_RATIO:
        return "Sparks must use a phone-like portrait aspect ratio."
    return None


def process_spark_video(video_id, source_path):
    """
    Normalize a spark upload to a phone portrait frame and under-60s duration.
    Returns (final_path, meta, error_message).
    """
    target_filename = f"{video_id}.mp4"
    target_path = os.path.join(UPLOAD_FOLDER, target_filename)
    temp_output_path = os.path.join(UPLOAD_FOLDER, f"{video_id}.spark_tmp.mp4")
    vf = (
        f"scale={SPARK_TARGET_WIDTH}:{SPARK_TARGET_HEIGHT}:force_original_aspect_ratio=increase,"
        f"crop={SPARK_TARGET_WIDTH}:{SPARK_TARGET_HEIGHT}"
    )
    try:
        result = subprocess.run(
            [
                "ffmpeg",
                "-y",
                "-i",
                source_path,
                "-vf",
                vf,
                "-t",
                str(SPARK_TARGET_DURATION_SECONDS),
                "-c:v",
                "libx264",
                "-preset",
                "veryfast",
                "-crf",
                "23",
                "-c:a",
                "aac",
                "-b:a",
                "128k",
                "-movflags",
                "+faststart",
                temp_output_path,
            ],
            capture_output=True,
            text=True,
            timeout=8 * 60,
            check=False,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return None, None, "Spark processing failed because ffmpeg is unavailable on the server."

    if result.returncode != 0 or not os.path.exists(temp_output_path):
        if os.path.exists(temp_output_path):
            os.remove(temp_output_path)
        return None, None, "Spark processing failed. Please try a different source video."

    if os.path.exists(target_path):
        os.remove(target_path)
    os.replace(temp_output_path, target_path)
    if os.path.abspath(source_path) != os.path.abspath(target_path) and os.path.exists(source_path):
        os.remove(source_path)

    meta = ffprobe_video(target_path)
    spark_error = validate_spark_meta(meta)
    if spark_error:
        if os.path.exists(target_path):
            os.remove(target_path)
        return None, None, f"Spark processing produced an invalid output: {spark_error}"

    return target_path, meta, None


def public_video(v, user_map=None):
    out = dict(v)
    out.pop("filename", None)
    out["kind"] = normalize_kind(v.get("kind"))
    if user_map is not None:
        uploader = user_map.get(v.get("user_id"))
        out["uploader_avatar_url"] = (uploader or {}).get("avatar_url", "")
    likes = out.get("likes")
    comments = out.get("comments")
    if not isinstance(likes, list):
        likes = []
    if not isinstance(comments, list):
        comments = []
    out["likes_count"] = len(likes)
    out["comments_count"] = len(comments)
    out.pop("likes", None)
    out.pop("comments", None)
    return out


def get_video_by_id(videos, video_id):
    return next((v for v in videos if v["id"] == video_id), None)


def video_with_viewer(v, viewer, user_map=None):
    if user_map is None:
        user_map = {u["id"]: u for u in load_users()}
    out = public_video(v, user_map)
    likes = v.get("likes")
    if not isinstance(likes, list):
        likes = []
    viewer_following = (viewer or {}).get("following") or []
    out["liked_by_me"] = bool(viewer and viewer["id"] in likes)
    out["uploader_is_subscribed"] = bool(viewer and v.get("user_id") in viewer_following)
    out["uploader_is_me"] = bool(viewer and v.get("user_id") == viewer.get("id"))
    return out


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.route("/")
def index(): return send_file("static/index.html")

@app.route("/watch")
def watch(): return send_file("static/watch.html")

@app.route("/upload")
def upload_page(): return send_file("static/upload.html")

@app.route("/login")
def login_page(): return send_file("static/login.html")

@app.route("/register")
def register_page(): return send_file("static/register.html")

@app.route("/profile")
def profile_page(): return send_file("static/profile.html")

@app.route("/favicon.ico")
def favicon():
    # Keep /favicon.ico explicit for browser defaults.
    return redirect("/favicon.svg", code=302)


@app.route("/favicon.svg")
def favicon_svg():
    # Force revalidation so favicon updates show up without hard refresh.
    response = send_from_directory(app.static_folder, "favicon.svg", mimetype="image/svg+xml")
    response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


# ── Auth API ──────────────────────────────────────────────────────────────────

@app.route("/api/auth/register", methods=["POST"])
def api_register():
    data = request.json or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")

    if not username or not password:
        return jsonify({"error": "Username and password required"}), 400
    if len(username) < 2 or len(username) > 30:
        return jsonify({"error": "Username must be 2-30 characters"}), 400
    if len(password) < 4:
        return jsonify({"error": "Password must be at least 4 characters"}), 400

    users = load_users()
    if any(u["username"].lower() == username.lower() for u in users):
        return jsonify({"error": "Username already taken"}), 409

    user = {
        "id": str(uuid.uuid4()),
        "username": username,
        "password_hash": generate_password_hash(password),
        "joined_at": datetime.utcnow().isoformat(),
        "avatar_url": "",
        "following": [],
    }
    users.append(user)
    save_users(users)

    session["user_id"] = user["id"]
    return jsonify({"id": user["id"], "username": user["username"], "avatar_url": user.get("avatar_url", "")})


@app.route("/api/auth/login", methods=["POST"])
def api_login():
    data = request.json or {}
    username = data.get("username", "").strip()
    password = data.get("password", "")

    users = load_users()
    user = next((u for u in users if u["username"].lower() == username.lower()), None)
    if not user or not check_password_hash(user["password_hash"], password):
        return jsonify({"error": "Invalid username or password"}), 401

    session["user_id"] = user["id"]
    return jsonify({"id": user["id"], "username": user["username"], "avatar_url": user.get("avatar_url", "")})


@app.route("/api/auth/logout", methods=["POST"])
def api_logout():
    session.clear()
    return jsonify({"message": "Logged out"})


@app.route("/api/auth/me")
def api_me():
    user = current_user()
    if not user:
        return jsonify({"user": None})
    return jsonify({"user": user_public(user, user)})


@app.route("/api/users/<user_id>")
def api_user(user_id):
    viewer = current_user()
    users = load_users()
    user = next((u for u in users if u["id"] == user_id), None)
    if not user:
        abort(404)
    return jsonify(user_public(user, viewer))


@app.route("/api/users/me", methods=["PATCH"])
def api_user_me_patch():
    user = current_user()
    if not user:
        return jsonify({"error": "Not logged in"}), 401
    data = request.json or {}
    avatar_url = str(data.get("avatar_url", "")).strip()
    if avatar_url and len(avatar_url) > 600:
        return jsonify({"error": "Avatar URL is too long"}), 400
    users = load_users()
    target = next((u for u in users if u["id"] == user["id"]), None)
    if not target:
        return jsonify({"error": "User not found"}), 404
    target["avatar_url"] = avatar_url
    save_users(users)
    return jsonify(user_public(target, target))


@app.route("/api/users/me/avatar", methods=["POST"])
def api_user_me_avatar():
    user = current_user()
    if not user:
        return jsonify({"error": "Not logged in"}), 401

    avatar = request.files.get("avatar")
    if not avatar or not avatar.filename:
        return jsonify({"error": "No avatar file uploaded"}), 400
    if not allowed_avatar_file(avatar.filename):
        return jsonify({"error": "Invalid image type. Use png, jpg, jpeg, webp, or gif."}), 400

    try:
        avatar.stream.seek(0, os.SEEK_END)
        size = avatar.stream.tell()
        avatar.stream.seek(0)
    except Exception:
        size = 0
    if size <= 0:
        return jsonify({"error": "Avatar file is empty"}), 400
    if size > MAX_AVATAR_FILE_SIZE:
        return jsonify({"error": "Avatar file is too large (max 5MB)"}), 400

    ext = avatar.filename.rsplit(".", 1)[1].lower()
    avatar_name = f"{user['id']}_{uuid.uuid4().hex[:10]}.{ext}"
    avatar_path = os.path.join(AVATAR_UPLOAD_FOLDER, avatar_name)
    avatar.save(avatar_path)

    users = load_users()
    target = next((u for u in users if u["id"] == user["id"]), None)
    if not target:
        if os.path.exists(avatar_path):
            os.remove(avatar_path)
        return jsonify({"error": "User not found"}), 404

    old_avatar = str(target.get("avatar_url") or "").strip()
    if old_avatar.startswith("/static/avatars/"):
        old_name = old_avatar[len("/static/avatars/"):].split("?", 1)[0]
        old_path = os.path.join(AVATAR_UPLOAD_FOLDER, old_name)
        if os.path.exists(old_path):
            try:
                os.remove(old_path)
            except OSError:
                pass

    target["avatar_url"] = f"/static/avatars/{avatar_name}"
    save_users(users)
    return jsonify(user_public(target, target))


@app.route("/api/users/<user_id>/subscribe", methods=["POST"])
def api_subscribe(user_id):
    viewer = current_user()
    if not viewer:
        return jsonify({"error": "You must be logged in to subscribe"}), 401
    if viewer["id"] == user_id:
        return jsonify({"error": "You cannot subscribe to yourself"}), 400

    users = load_users()
    target = next((u for u in users if u["id"] == user_id), None)
    actor = next((u for u in users if u["id"] == viewer["id"]), None)
    if not target or not actor:
        abort(404)

    following = actor.get("following")
    if not isinstance(following, list):
        following = []
    if user_id in following:
        following = [uid for uid in following if uid != user_id]
        subscribed = False
    else:
        following.append(user_id)
        subscribed = True
    actor["following"] = following
    save_users(users)

    subscribers_count = sum(1 for u in users if user_id in (u.get("following") or []))
    return jsonify({"subscribed": subscribed, "subscribers_count": subscribers_count})


# ── Videos API ───────────────────────────────────────────────────────────────

@app.route("/api/videos")
def api_videos():
    viewer = current_user()
    videos = load_videos()
    user_map = {u["id"]: u for u in load_users()}
    q = request.args.get("q", "").lower()
    user_id = request.args.get("user_id", "")
    kind = normalize_kind(request.args.get("kind")) if request.args.get("kind") else ""
    if q:
        videos = [v for v in videos if q in v["title"].lower() or q in v.get("description", "").lower()]
    if user_id:
        videos = [v for v in videos if v.get("user_id") == user_id]
    if kind:
        videos = [v for v in videos if normalize_kind(v.get("kind")) == kind]
    videos = sorted(videos, key=lambda v: v["uploaded_at"], reverse=True)
    return jsonify([video_with_viewer(v, viewer, user_map) for v in videos])


@app.route("/api/videos/<video_id>")
def api_video(video_id):
    viewer = current_user()
    videos = load_videos()
    user_map = {u["id"]: u for u in load_users()}
    video = get_video_by_id(videos, video_id)
    if not video:
        abort(404)
    video["views"] = video.get("views", 0) + 1
    save_videos(videos)
    out = video_with_viewer(video, viewer, user_map)
    out["comments"] = video.get("comments", []) if isinstance(video.get("comments"), list) else []
    return jsonify(out)


@app.route("/api/videos/<video_id>/like", methods=["POST"])
def api_like_video(video_id):
    user = current_user()
    if not user:
        return jsonify({"error": "You must be logged in to like videos"}), 401

    videos = load_videos()
    video = get_video_by_id(videos, video_id)
    if not video:
        abort(404)

    likes = video.get("likes")
    if not isinstance(likes, list):
        likes = []

    if user["id"] in likes:
        likes = [uid for uid in likes if uid != user["id"]]
        liked = False
    else:
        likes.append(user["id"])
        liked = True

    video["likes"] = likes
    save_videos(videos)
    return jsonify({"liked": liked, "likes_count": len(likes)})


@app.route("/api/videos/<video_id>/comments")
def api_video_comments(video_id):
    users = load_users()
    user_map = {u["id"]: u for u in users}
    videos = load_videos()
    video = get_video_by_id(videos, video_id)
    if not video:
        abort(404)
    comments = video.get("comments")
    if not isinstance(comments, list):
        comments = []
    out = []
    for c in comments:
        item = dict(c)
        comment_user = user_map.get(item.get("user_id"))
        if comment_user:
            item["username"] = comment_user.get("username") or item.get("username") or "User"
            item["avatar_url"] = comment_user.get("avatar_url", "")
        else:
            item["avatar_url"] = ""
            if not item.get("username"):
                item["username"] = "User"
        out.append(item)
    out = sorted(out, key=lambda c: c.get("created_at", ""), reverse=True)
    return jsonify(out)


@app.route("/api/videos/<video_id>/comments", methods=["POST"])
def api_add_comment(video_id):
    user = current_user()
    if not user:
        return jsonify({"error": "You must be logged in to comment"}), 401

    data = request.json or {}
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "Comment cannot be empty"}), 400
    if len(text) > 800:
        return jsonify({"error": "Comment is too long (max 800 chars)"}), 400

    videos = load_videos()
    video = get_video_by_id(videos, video_id)
    if not video:
        abort(404)

    comments = video.get("comments")
    if not isinstance(comments, list):
        comments = []

    comment = {
        "id": str(uuid.uuid4()),
        "user_id": user["id"],
        "username": user["username"],
        "avatar_url": user.get("avatar_url", ""),
        "text": text,
        "created_at": datetime.utcnow().isoformat(),
    }
    comments.append(comment)
    video["comments"] = comments
    save_videos(videos)
    return jsonify(comment), 201


@app.route("/api/users/<user_id>/comments")
def api_user_comments(user_id):
    users = load_users()
    user = next((u for u in users if u["id"] == user_id), None)
    if not user:
        abort(404)

    target_username = str(user.get("username") or "").strip().lower()
    videos = load_videos()
    out = []
    for v in videos:
        comments = v.get("comments")
        if not isinstance(comments, list):
            continue
        for c in comments:
            comment_user_id = c.get("user_id")
            comment_username = str(c.get("username") or "").strip().lower()
            if comment_user_id != user_id and not (not comment_user_id and target_username and comment_username == target_username):
                continue
            out.append({
                "id": c.get("id") or str(uuid.uuid4()),
                "user_id": user_id,
                "username": user.get("username") or c.get("username") or "User",
                "avatar_url": user.get("avatar_url", ""),
                "text": c.get("text") or "",
                "created_at": c.get("created_at") or "",
                "video_id": v.get("id"),
                "video_title": v.get("title") or "Untitled",
                "video_kind": normalize_kind(v.get("kind")),
            })

    out = sorted(out, key=lambda c: c.get("created_at", ""), reverse=True)
    return jsonify(out)


@app.route("/api/upload", methods=["POST"])
def api_upload():
    user = current_user()
    if not user:
        return jsonify({"error": "You must be logged in to upload"}), 401

    if "video" not in request.files:
        return jsonify({"error": "No video file"}), 400
    file = request.files["video"]
    title = request.form.get("title", "").strip()
    description = request.form.get("description", "").strip()
    kind = normalize_kind(request.form.get("kind"))
    hinted_meta = parse_video_meta(request.form.get("video_meta"))

    if not title:
        return jsonify({"error": "Title is required"}), 400
    if not file or not allowed_file(file.filename):
        return jsonify({"error": "Invalid file type"}), 400

    video_id = str(uuid.uuid4())
    ext = file.filename.rsplit(".", 1)[1].lower()
    filename = f"{video_id}.{ext}"
    saved_path = os.path.join(UPLOAD_FOLDER, filename)
    file.save(saved_path)

    actual_meta = ffprobe_video(saved_path) or hinted_meta
    if kind == "spark":
        processed_path, processed_meta, spark_error = process_spark_video(video_id, saved_path)
        if spark_error:
            if os.path.exists(saved_path):
                os.remove(saved_path)
            return jsonify({"error": spark_error}), 400
        saved_path = processed_path
        filename = os.path.basename(saved_path)
        actual_meta = processed_meta

    video = {
        "id": video_id,
        "title": title,
        "description": description,
        "uploader": user["username"],
        "user_id": user["id"],
        "filename": filename,
        "kind": kind,
        "uploaded_at": datetime.utcnow().isoformat(),
        "views": 0,
        "likes": [],
        "comments": [],
        "file_size": os.path.getsize(saved_path),
    }
    if actual_meta:
        video["duration"] = actual_meta["duration"]
        video["width"] = actual_meta["width"]
        video["height"] = actual_meta["height"]

    videos = load_videos()
    videos.append(video)
    save_videos(videos)
    return jsonify({"id": video_id, "message": "Uploaded successfully"})


@app.route("/api/upload/chunk/start", methods=["POST"])
def api_upload_chunk_start():
    user = current_user()
    if not user:
        return jsonify({"error": "You must be logged in to upload"}), 401

    data = request.json or {}
    original_filename = (data.get("filename") or "").strip()
    title = (data.get("title") or "").strip()
    description = (data.get("description") or "").strip()
    kind = normalize_kind(data.get("kind"))
    hinted_meta = parse_video_meta(data.get("video_meta"))
    try:
        file_size = int(data.get("file_size") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid file size"}), 400

    if not title:
        return jsonify({"error": "Title is required"}), 400
    if not original_filename or not allowed_file(original_filename):
        return jsonify({"error": "Invalid file type"}), 400
    if file_size <= 0:
        return jsonify({"error": "Invalid file size"}), 400
    if file_size > MAX_CONTENT_LENGTH:
        max_mb = MAX_CONTENT_LENGTH // (1024 * 1024)
        return jsonify({"error": f"File too large. Maximum upload size is {max_mb}MB."}), 413

    upload_id = str(uuid.uuid4())
    session_dir = upload_session_path(upload_id)
    os.makedirs(session_dir, exist_ok=True)

    ext = original_filename.rsplit(".", 1)[1].lower()
    meta = {
        "upload_id": upload_id,
        "user_id": user["id"],
        "uploader": user["username"],
        "filename_original": original_filename,
        "ext": ext,
        "title": title,
        "description": description,
        "kind": kind,
        "hinted_meta": hinted_meta,
        "file_size": file_size,
        "created_at": datetime.utcnow().isoformat(),
    }
    save_upload_meta(upload_id, meta)
    return jsonify({"upload_id": upload_id, "chunk_size": CHUNK_SIZE})


@app.route("/api/upload/chunk/part", methods=["POST"])
def api_upload_chunk_part():
    user = current_user()
    if not user:
        return jsonify({"error": "You must be logged in to upload"}), 401

    upload_id = (request.form.get("upload_id") or "").strip()
    if not upload_id:
        return jsonify({"error": "Missing upload_id"}), 400

    meta = load_upload_meta(upload_id)
    if not meta:
        return jsonify({"error": "Upload session not found"}), 404
    if meta.get("user_id") != user["id"]:
        return jsonify({"error": "Upload session does not belong to you"}), 403

    chunk_index_raw = request.form.get("chunk_index", "").strip()
    if not chunk_index_raw.isdigit():
        return jsonify({"error": "Invalid chunk index"}), 400
    chunk_index = int(chunk_index_raw)

    chunk = request.files.get("chunk")
    if not chunk:
        return jsonify({"error": "Missing chunk"}), 400

    part_path = os.path.join(upload_session_path(upload_id), f"chunk_{chunk_index:06d}.part")
    chunk.save(part_path)
    return jsonify({"ok": True, "chunk_index": chunk_index})


@app.route("/api/upload/chunk/complete", methods=["POST"])
def api_upload_chunk_complete():
    user = current_user()
    if not user:
        return jsonify({"error": "You must be logged in to upload"}), 401

    data = request.json or {}
    upload_id = (data.get("upload_id") or "").strip()
    try:
        total_chunks = int(data.get("total_chunks") or 0)
    except (TypeError, ValueError):
        return jsonify({"error": "Invalid total chunks"}), 400

    if not upload_id:
        return jsonify({"error": "Missing upload_id"}), 400
    if total_chunks <= 0:
        return jsonify({"error": "Invalid total chunks"}), 400

    meta = load_upload_meta(upload_id)
    if not meta:
        return jsonify({"error": "Upload session not found"}), 404
    if meta.get("user_id") != user["id"]:
        return jsonify({"error": "Upload session does not belong to you"}), 403

    session_dir = upload_session_path(upload_id)
    part_paths = [os.path.join(session_dir, f"chunk_{i:06d}.part") for i in range(total_chunks)]
    missing = [i for i, path in enumerate(part_paths) if not os.path.exists(path)]
    if missing:
        return jsonify({"error": f"Missing chunks: {missing[:5]}"}), 400

    video_id = str(uuid.uuid4())
    filename = f"{video_id}.{meta['ext']}"
    final_path = os.path.join(UPLOAD_FOLDER, filename)

    with open(final_path, "wb") as out:
        for path in part_paths:
            with open(path, "rb") as part:
                out.write(part.read())

    for path in part_paths:
        if os.path.exists(path):
            os.remove(path)
    meta_file = upload_meta_path(upload_id)
    if os.path.exists(meta_file):
        os.remove(meta_file)
    if os.path.isdir(session_dir):
        os.rmdir(session_dir)

    actual_meta = ffprobe_video(final_path) or parse_video_meta(meta.get("hinted_meta"))
    kind = normalize_kind(meta.get("kind"))
    if kind == "spark":
        processed_path, processed_meta, spark_error = process_spark_video(video_id, final_path)
        if spark_error:
            if os.path.exists(final_path):
                os.remove(final_path)
            return jsonify({"error": spark_error}), 400
        final_path = processed_path
        filename = os.path.basename(final_path)
        actual_meta = processed_meta

    video = {
        "id": video_id,
        "title": meta["title"],
        "description": meta["description"],
        "uploader": user["username"],
        "user_id": user["id"],
        "filename": filename,
        "kind": kind,
        "uploaded_at": datetime.utcnow().isoformat(),
        "views": 0,
        "likes": [],
        "comments": [],
        "file_size": os.path.getsize(final_path),
    }
    if actual_meta:
        video["duration"] = actual_meta["duration"]
        video["width"] = actual_meta["width"]
        video["height"] = actual_meta["height"]

    videos = load_videos()
    videos.append(video)
    save_videos(videos)
    return jsonify({"id": video_id, "message": "Uploaded successfully"})


@app.errorhandler(RequestEntityTooLarge)
def handle_file_too_large(_):
    max_mb = MAX_CONTENT_LENGTH // (1024 * 1024)
    return jsonify({"error": f"File too large. Maximum upload size is {max_mb}MB."}), 413


@app.route("/uploads/<path:filename>")
def serve_video(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


@app.route("/api/stream/<video_id>")
def api_stream(video_id):
    videos = load_videos()
    video = next((v for v in videos if v["id"] == video_id), None)
    if not video:
        abort(404)

    filepath = os.path.join(UPLOAD_FOLDER, video["filename"])
    if not os.path.exists(filepath):
        abort(404)

    mimetype, _ = mimetypes.guess_type(filepath)
    ext = os.path.splitext(video.get("filename", ""))[1] or ".mp4"
    raw_title = (video.get("title") or "video").strip()
    safe_title = "".join(c if c.isalnum() or c in ("-", "_", ".") else "_" for c in raw_title).strip("._")
    download_name = f"{safe_title or 'video'}{ext}"
    as_attachment = request.args.get("download", "").lower() in {"1", "true", "yes"}
    response = send_file(
        filepath,
        mimetype=mimetype or "video/mp4",
        conditional=True,
        as_attachment=as_attachment,
        download_name=download_name,
    )
    response.headers["Accept-Ranges"] = "bytes"
    if not as_attachment:
        response.headers["Content-Disposition"] = f'inline; filename="{download_name}"'
    response.headers["Cache-Control"] = "private, no-store, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response


@app.route("/api/videos/<video_id>", methods=["DELETE"])
def api_delete(video_id):
    user = current_user()
    if not user:
        return jsonify({"error": "Not logged in"}), 401

    videos = load_videos()
    video = next((v for v in videos if v["id"] == video_id), None)
    if not video:
        abort(404)
    if video.get("user_id") != user["id"]:
        return jsonify({"error": "You can only delete your own videos"}), 403

    filepath = os.path.join(UPLOAD_FOLDER, video["filename"])
    if os.path.exists(filepath):
        os.remove(filepath)
    save_videos([v for v in videos if v["id"] != video_id])
    return jsonify({"message": "Deleted"})


if __name__ == "__main__":
    preferred_port = int(os.environ.get("PORT", "5000"))
    selected_port = preferred_port

    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.bind(("127.0.0.1", preferred_port))
    except OSError:
        fallback = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            fallback.bind(("127.0.0.1", 0))
            selected_port = fallback.getsockname()[1]
        finally:
            fallback.close()
        print(f"[WARN] Port {preferred_port} is in use. Falling back to port {selected_port}.")
    finally:
        probe.close()

    app.run(debug=True, port=selected_port)

