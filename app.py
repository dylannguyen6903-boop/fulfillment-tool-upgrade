import os
import json
import threading
import queue
import time
import gspread
from flask import Flask, render_template, request, jsonify, session, redirect, url_for, Response
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
import re

app = Flask(__name__)
# Mã bí mật bảo mật phiên làm việc
app.secret_key = os.environ.get('FLASK_SECRET_KEY', 'daisy_synergy_final_v692_stable_v2')

# ======================== CẤU HÌNH GOOGLE OAUTH v6.9.2 ========================
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
os.environ['OAUTHLIB_RELAX_TOKEN_SCOPE'] = '1'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive.readonly']
progress_streams = {}

def get_flow():
    client_id = os.environ.get('GOOGLE_CLIENT_ID', '').strip()
    client_secret = os.environ.get('GOOGLE_CLIENT_SECRET', '').strip()
    
    if not client_id or not client_secret:
        raise ValueError("Thiếu biến môi trường GOOGLE_CLIENT_ID hoặc GOOGLE_CLIENT_SECRET trên Render!")

    host = os.environ.get('RENDER_EXTERNAL_HOSTNAME', request.host)
    url = f"https://{host}/callback" if 'localhost' not in host else f"http://{host}/callback"
    
    client_config = {
        "web": {
            "client_id": client_id,
            "client_secret": client_secret,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "redirect_uris": [url, f"https://{host}/oauth2callback"]
        }
    }
    
    redirect_url = url_for('callback', _external=True)
    if 'localhost' not in request.host and redirect_url.startswith('http://'):
        redirect_url = redirect_url.replace('http://', 'https://', 1)
        
    return Flow.from_client_config(client_config, scopes=SCOPES, state=session.get('state'), redirect_uri=redirect_url)

# ======================== DRIVE LOGIC v6.0 ========================

def get_all_files_recursive(drive_service, folder_id, is_base_scan=False):
    all_f = []
    to_p = [folder_id]
    p = set()
    while to_p:
        fid = to_p.pop(0)
        if fid in p: continue
        p.add(fid)
        q = f"'{fid}' in parents and trashed = false"
        tk = None
        while True:
            try:
                res = drive_service.files().list(q=q, supportsAllDrives=True, includeItemsFromAllDrives=True, fields='nextPageToken, files(id, name, webViewLink, mimeType)', pageSize=200, pageToken=tk).execute()
                for f in res.get('files', []):
                    if f['mimeType'] == 'application/vnd.google-apps.folder':
                        if is_base_scan:
                            fn = f['name'].upper()
                            if fn.startswith('#') or re.search(r'FR[0-9]+', fn) or re.search(r'#[A-Z0-9]+', fn): continue
                        to_p.append(f['id'])
                    else:
                        all_f.append(f)
                tk = res.get('nextPageToken')
                if not tk: break
            except:
                break
    return all_f

def find_sku_folder(drive_service, sku, target_drive_id=None):
    clean = sku.strip().replace("'", "\\'")
    def do_search(q):
        if target_drive_id:
            try:
                return drive_service.files().list(q=q, corpora='drive', driveId=target_drive_id, supportsAllDrives=True, includeItemsFromAllDrives=True, fields='files(id, name, webViewLink, mimeType, driveId)').execute().get('files', [])
            except:
                return []
        return drive_service.files().list(q=q, supportsAllDrives=True, includeItemsFromAllDrives=True, fields='files(id, name, webViewLink, mimeType)').execute().get('files', [])
    
    for q in [f"name = '{clean}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false", f"name contains '{clean}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"]:
        folders = do_search(q)
        if folders:
            if target_drive_id:
                tg = [f for f in folders if f.get('driveId') == target_drive_id]
                if tg: folders = tg
            return folders[0]
    return None

def find_order_subfolder(drive_service, parent_id, order_number):
    if not order_number: return None
    tp = re.findall(r'[A-Za-z0-9]+', order_number.upper())
    if not tp: return None
    st = max(tp, key=len)
    q = f"'{parent_id}' in parents and name contains '{st}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    try:
        folders = drive_service.files().list(q=q, supportsAllDrives=True, includeItemsFromAllDrives=True, fields='files(id, name, webViewLink, mimeType)').execute().get('files', [])
        for f in folders:
            fp = re.findall(r'[A-Za-z0-9]+', f['name'].upper())
            if all(p in fp for p in tp): return f
    except:
        pass
    return None

def get_fulfillment_drive_id(drive_service):
    try:
        results = drive_service.drives().list(pageSize=100).execute()
        for d in results.get('drives', []):
            if 'FULFILLMENT' in d.get('name', '').upper(): return d.get('id')
    except:
        pass
    return None

# ======================== WEB ROUTES v6.9.2 ========================

@app.route('/')
def index():
    return render_template('index.html', authenticated=('credentials' in session))

@app.route('/login')
def login():
    flow = get_flow()
    auth_url, state = flow.authorization_url(access_type='offline', include_granted_scopes='true', prompt='consent')
    session['state'] = state
    if hasattr(flow, 'code_verifier'):
        session['code_verifier'] = flow.code_verifier
    return redirect(auth_url)

@app.route('/callback')
@app.route('/oauth2callback')
def callback():
    try:
        flow = get_flow()
        auth_url = request.url
        if 'localhost' not in request.host and auth_url.startswith('http://'):
            auth_url = auth_url.replace('http://', 'https://', 1)
        flow.fetch_token(authorization_response=auth_url, code_verifier=session.get('code_verifier'))
        c = flow.credentials
        session['credentials'] = {'token': c.token, 'refresh_token': c.refresh_token, 'token_uri': c.token_uri, 'client_id': c.client_id, 'client_secret': c.client_secret, 'scopes': c.scopes}
        return redirect(url_for('index'))
    except Exception as e:
        import traceback
        return f"<h1>Lỗi đăng nhập:</h1><p>{str(e)}</p><pre>{traceback.format_exc()}</pre>"

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('index'))

@app.route('/get_tabs', methods=['POST'])
def get_tabs():
    try:
        if 'credentials' not in session: return jsonify({'error': 'Chưa đăng nhập!'})
        creds = Credentials(**session.get('credentials'))
        gc = gspread.authorize(creds)
        sh = gc.open_by_url(request.json.get('sheet_url'))
        return jsonify({'tabs': [{'index': i, 'title': ws.title} for i, ws in enumerate(sh.worksheets())]})
    except Exception as e: return jsonify({'error': str(e)})

@app.route('/run', methods=['POST'])
def run_tool():
    if 'credentials' not in session: return jsonify({'error': 'Chưa đăng nhập!'})
    stream_id = str(int(time.time()))
    progress_streams[stream_id] = queue.Queue()
    threading.Thread(target=worker_loop_final, args=(session.get('credentials'), request.json.get('sheet_url'), int(request.json.get('tab_index', 0)), stream_id)).start()
    return jsonify({'stream_id': stream_id})

def worker_loop_final(creds_data, sheet_url, tab_index, stream_id):
    q = progress_streams.get(stream_id)
    def log(m, t='info'): q.put(json.dumps({'type': t, 'message': m}))
    try:
        creds = Credentials(**creds_data)
        gc = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)
        sh = gc.open_by_url(sheet_url)
        ws = sh.worksheets()[tab_index]
        all_v = ws.get_all_values()
        drive_id = get_fulfillment_drive_id(drive_service)
        sku_list = [(i, r[9].strip()) for i, r in enumerate(all_v) if i > 0 and len(r) > 9 and r[9].strip()]
        sku_cache = {}
        updates = []
        
        for i, (ridx, sku) in enumerate(sku_list):
            order = all_v[ridx][1].strip() if len(all_v[ridx]) > 1 else ""
            mapping = {k: "" for k in ['MK', 'Collar', 'Left Sleeve', 'Right Sleeve', 'Front', 'Back']}
            mf = sku_cache.get(sku) or find_sku_folder(drive_service, sku, drive_id)
            sku_cache[sku] = mf
            if not mf:
                log(f"[{i+1}/{len(sku_list)}] {sku} -> ❌", 'warning')
                continue
            
            of = find_order_subfolder(drive_service, mf['id'], order)
            v_b = get_all_files_recursive(drive_service, mf['id'], is_base_scan=True)
            v_c = get_all_files_recursive(drive_service, of['id'], is_base_scan=False) if of else []
            
            def filt(files):
                res = []
                for f in files:
                    n = f['name'].upper()
                    m = f.get('mimeType', '')
                    if any(n.endswith(ext) for ext in ['.PSD','.PSB']) or 'photoshop' in m: continue
                    if any(n.endswith(ext) for ext in ['.JPG','.PNG','.JPEG','.WEBP']) or m.startswith('image/'): res.append(f)
                return res
            
            base = filt(v_b)
            cust = filt(v_c)
            
            def match_logic(imgs, over=False):
                reg_l = r'(?i)(^|[^A-Z0-9])(L|LEFT|TRAI|TAY T)([^A-Z0-9]|$)'
                reg_r = r'(?i)(^|[^A-Z0-9])(R|RIGHT|PHAI|TAY P)([^A-Z0-9]|$)'
                for f in imgs:
                    n = f['name'].upper()
                    l = f['webViewLink']
                    if any(k in n for k in ['MK','MOC','DEMO','MOCKUP']):
                        if not mapping['MK'] or over: mapping['MK'] = l
                    elif any(k in n for k in ['COLLAR',' CO',' CỔ']):
                        if not mapping['Collar'] or over: mapping['Collar'] = l
                    elif any(k in n for k in ['FRONT','TRUOC','TRƯỚC']):
                        if not mapping['Front'] or over: mapping['Front'] = l
                    elif any(k in n for k in ['BACK','SAU']):
                        if not mapping['Back'] or over: mapping['Back'] = l
                    elif 'SLEEVE' in n or 'TAY' in n or re.search(reg_l, n) or re.search(reg_r, n):
                        if re.search(reg_l, n) or any(k in n for k in ['LEFT','TRAI','TAY T']):
                            if not mapping['Left Sleeve'] or over: mapping['Left Sleeve'] = l
                        elif re.search(reg_r, n) or any(k in n for k in ['RIGHT','PHAI','TAY P']):
                            if not mapping['Right Sleeve'] or over: mapping['Right Sleeve'] = l
            
            match_logic(base, False)
            match_logic(cust, True)
            
            up = [mapping[k] for k in ['MK', 'Collar', 'Left Sleeve', 'Right Sleeve', 'Front', 'Back']]
            updates.append({'range': f"V{ridx+1}:AA{ridx+1}", 'values': [up]})
            log(f"[{i+1}/{len(sku_list)}] {sku} ({order}) -> ✅", 'success')
            
        if updates:
            ws.batch_update(updates)
        log('✨ HOÀN THÀNH!', 'success')
        log('DONE', 'done')
    except Exception as e:
        log(f'❌ Lỗi: {e}', 'error')
        log('DONE', 'done')

@app.route('/progress/<stream_id>')
def progress(stream_id):
    def gen():
        q = progress_streams.get(stream_id)
        while q:
            msg = q.get()
            yield f"data: {msg}\n\n"
            if json.loads(msg).get('type') == 'done': break
    return Response(gen(), mimetype='text/event-stream')

if __name__ == '__main__':
    app.run(debug=True, port=int(os.environ.get('PORT', 5000)))
