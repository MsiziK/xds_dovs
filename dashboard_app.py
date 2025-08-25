from flask import Flask, render_template, jsonify, request, send_from_directory, Response
from flask import redirect, url_for
import os
from datetime import datetime
import csv
import io
import db_access
from reportlab.lib.pagesizes import letter
from reportlab.pdfgen import canvas
import xlsxwriter

app = Flask(__name__)

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "verifications.db")
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

# ---------------- CUSTOM URLS ---------------- #
DASHBOARD_URL = "/admin/dashboard"
CLIENT_VERIFICATION_URL = "/verify"


def get_full_upload_path(path_value):
    """Return the absolute path on disk for any stored upload."""
    if not path_value:
        return None
    filename = os.path.basename(path_value)
    full_path = os.path.join(UPLOAD_FOLDER, filename)
    return full_path if os.path.exists(full_path) else None


def _to_uploads_url(path_value):
    """Return a browser-usable URL under /uploads for any stored path."""
    if not path_value:
        return ""
    p = str(path_value).strip().replace("\\", "/")
    return f"/{p}" if not p.startswith("/") else p


@app.route("/uploads/<path:filename>")
def uploaded_file(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)


@app.route("/")
def root():
    # Redirect root requests to the main dashboard
    return redirect(url_for("index"))

# ---------------- DASHBOARD ---------------- #


@app.route(DASHBOARD_URL)
def index():
    # --- Collect filters from query params ---
    status = request.args.get("status", "all")
    name = request.args.get("name", "").strip()
    id_number = request.args.get("id_number", "").strip()
    month = request.args.get("month", "0")
    year = request.args.get("year", str(datetime.now().year))
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")

    current_filters = {
        "status": status,
        "name": name,
        "id_number": id_number,
        "month": int(month) if month.isdigit() else 0,
        "year": int(year) if year.isdigit() else datetime.now().year,
        "date_from": date_from,
        "date_to": date_to,
    }

    logs = db_access.fetch_all_verifications()

    # --- Normalize photo fields for the template ---
    for v in logs:
        v["id_photo"] = _to_uploads_url(v.get("id_photo"))
        v["selfie_photo"] = _to_uploads_url(v.get("selfie_photo"))

    # --- Apply filters ---
    if status.lower() != "all":
        logs = [v for v in logs if v.get("status", "").lower() == status.lower()]
    if name:
        logs = [v for v in logs if name.lower() in v.get("name", "").lower()]
    if id_number:
        logs = [v for v in logs if v.get("id_number", "") == id_number]
    if current_filters["month"] != 0:
        logs = [v for v in logs if "timestamp" in v and datetime.strptime(v["timestamp"], "%Y-%m-%d %H:%M:%S").month == current_filters["month"]]
    if current_filters["year"]:
        logs = [v for v in logs if "timestamp" in v and datetime.strptime(v["timestamp"], "%Y-%m-%d %H:%M:%S").year == current_filters["year"]]
    if date_from:
        try:
            df = datetime.strptime(date_from, "%Y-%m-%d")
            logs = [v for v in logs if datetime.strptime(v["timestamp"], "%Y-%m-%d %H:%M:%S") >= df]
        except Exception:
            pass
    if date_to:
        try:
            dt = datetime.strptime(date_to, "%Y-%m-%d")
            logs = [v for v in logs if datetime.strptime(v["timestamp"], "%Y-%m-%d %H:%M:%S") <= dt]
        except Exception:
            pass

    # --- Stats ---
    total = len(logs)
    success = sum(1 for v in logs if v.get("status", "").lower() == "success")
    failed = sum(1 for v in logs if v.get("status", "").lower() == "failed")
    last_date = max([v.get("timestamp") for v in logs], default="N/A")

    stats = {"total": total, "success": success, "failed": failed, "last_date": last_date}

    # --- Chart data ---
    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month_values = [0] * 12
    for v in logs:
        try:
            ts = datetime.strptime(v["timestamp"], "%Y-%m-%d %H:%M:%S")
            month_values[ts.month - 1] += 1
        except Exception:
            continue

    return render_template(
        "dashboard.html",
        logs=logs,
        current_filters=current_filters,
        stats=stats,
        is_admin=True,
        current_year=datetime.now().year,
        month_labels=month_labels,
        month_values=month_values,
        current_query=request.query_string.decode("utf-8"),
        DASHBOARD_URL=DASHBOARD_URL,
    )


# ---------------- CLIENT VERIFICATION ---------------- #
@app.route(CLIENT_VERIFICATION_URL, methods=["GET", "POST"])
def client_verification():
    if request.method == "POST":
        data = request.get_json()
        id_number = data.get("id_number")
        cellphone = data.get("cellphone")
        # Call your verification logic here (replace with actual logic)
        ok = db_access.check_client_verification(id_number, cellphone)
        if ok:
            return jsonify({"success": True, "message": "Verification started successfully."})
        else:
            return jsonify({"success": False, "message": "ID or cellphone not recognized."})
    else:
        return render_template("client_verification.html")


# ---------------- DELETE VERIFICATIONS ---------------- #
@app.route("/delete_by_id_number/<id_number>", methods=["POST", "DELETE"])
def delete_by_id_number_route(id_number):
    logs = db_access.fetch_all_verifications()

    # --- Remove associated photos ---
    for v in logs:
        if v.get("id_number") == id_number:
            for photo_field in ["id_photo", "selfie_photo"]:
                path = v.get(photo_field)
                if path:
                    full_path = os.path.join(UPLOAD_FOLDER, os.path.basename(path))
                    if os.path.exists(full_path):
                        try:
                            os.remove(full_path)
                        except Exception as e:
                            print(f"[DEBUG] Could not delete {full_path}: {e}")

    # --- Delete from DB & audit log ---
    ok = db_access.delete_by_id_number(id_number)
    msg = f"Verification for ID Number {id_number} deleted from DB, audit log, and uploads."
    return jsonify({"success": bool(ok), "message": msg}), 200 if ok else 500


# ---------------- EXPORT ROUTES ---------------- #
@app.route("/export/csv")
def export_csv():
    logs = db_access.fetch_all_verifications()
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Timestamp", "Client ID", "Status", "Name", "ID Number", "Email", "ID Photo", "Selfie Photo"])
    for v in logs:
        writer.writerow([
            v.get("timestamp", ""),
            v.get("client_id", ""),
            v.get("status", ""),
            v.get("name", ""),
            v.get("id_number", ""),
            v.get("email", ""),
            v.get("id_photo", ""),
            v.get("selfie_photo", "")
        ])
    return Response(output.getvalue(), mimetype="text/csv",
                    headers={"Content-Disposition": "attachment;filename=verifications.csv"})


@app.route("/export/xlsx")
def export_xlsx():
    logs = db_access.fetch_all_verifications()
    output = io.BytesIO()
    workbook = xlsxwriter.Workbook(output, {"in_memory": True})
    worksheet = workbook.add_worksheet("Verifications")

    headers = ["Timestamp", "Client ID", "Status", "Name", "ID Number", "Email", "ID Photo", "Selfie Photo"]
    for col, h in enumerate(headers):
        worksheet.write(0, col, h)

    for row, v in enumerate(logs, start=1):
        worksheet.write(row, 0, v.get("timestamp", ""))
        worksheet.write(row, 1, v.get("client_id", ""))
        worksheet.write(row, 2, v.get("status", ""))
        worksheet.write(row, 3, v.get("name", ""))
        worksheet.write(row, 4, v.get("id_number", ""))
        worksheet.write(row, 5, v.get("email", ""))

        # --- Embed images if they exist ---
        if v.get("id_photo"):
            img_path = get_full_upload_path(v["id_photo"])
            if img_path:
                worksheet.set_row(row, 60)
                worksheet.insert_image(row, 6, img_path, {"x_scale": 0.3, "y_scale": 0.3})

        if v.get("selfie_photo"):
            img_path = get_full_upload_path(v["selfie_photo"])
            if img_path:
                worksheet.set_row(row, 60)
                worksheet.insert_image(row, 7, img_path, {"x_scale": 0.3, "y_scale": 0.3})

    workbook.close()
    output.seek(0)
    return Response(
        output.read(),
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment;filename=verifications.xlsx"},
    )


@app.route("/export/pdf")
def export_pdf():
    logs = db_access.fetch_all_verifications()
    output = io.BytesIO()
    c = canvas.Canvas(output, pagesize=letter)
    width, height = letter

    y = height - 40
    c.setFont("Helvetica-Bold", 12)
    c.drawString(30, y, "Verification Report")
    y -= 30

    c.setFont("Helvetica", 9)
    for v in logs:
        line = f"{v.get('timestamp', '')} | {v.get('client_id', '')} | {v.get('status', '')} | {v.get('name', '')} | {v.get('id_number', '')} | {v.get('email', '')}"
        c.drawString(30, y, line)
        y -= 15

        # --- Draw ID photo ---
        if v.get("id_photo"):
            img_path = get_full_upload_path(v["id_photo"])
            if img_path:
                try:
                    c.drawImage(img_path, 50, y - 60, width=80, height=60, preserveAspectRatio=True, mask="auto")
                except Exception as e:
                    c.drawString(50, y, f"[Could not render ID Photo: {e}]")

        # --- Draw Selfie photo ---
        if v.get("selfie_photo"):
            img_path = get_full_upload_path(v["selfie_photo"])
            if img_path:
                try:
                    c.drawImage(img_path, 150, y - 60, width=80, height=60, preserveAspectRatio=True, mask="auto")
                except Exception as e:
                    c.drawString(150, y, f"[Could not render Selfie Photo: {e}]")

        y -= 10
        if y < 80:
            c.showPage()
            y = height - 40
            c.setFont("Helvetica", 9)

    c.save()
    output.seek(0)
    return Response(
        output.read(),
        mimetype="application/pdf",
        headers={"Content-Disposition": "attachment;filename=verifications.pdf"},
    )


if __name__ == "__main__":
    app.run(debug=True)
