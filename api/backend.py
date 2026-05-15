        return jsonify({'error': 'image_base64 required'}), 400
    if not api_key:
        return jsonify({'error': 'ANTHROPIC_API_KEY not configured'}), 503
    payload = {
        'model': 'claude-sonnet-4-20250514',
        'max_tokens': 2000,
        'messages': [{
            'role': 'user',
            'content': [
                {
                    'type': 'image',
                    'source': {'type': 'base64', 'media_type': mime_type, 'data': image_b64}
                },
                {
                    'type': 'text',
                    'text': 'Extract from this Purchase Order document: PO Number, PO Date, Customer Name, Total Value (in INR), Payment Terms, Delivery Address, Scope of Work, OEM/Brand, Vendor/Distributor, line items, and any risk observations. Return a clean structured summary with clear section headers.'
                }
            ]
        }]
    }
    req = urllib.request.Request(
        'https://api.anthropic.com/v1/messages',
        data=json.dumps(payload).encode('utf-8'),
        headers={
            'Content-Type': 'application/json',
            'x-api-key': api_key,
            'anthropic-version': '2023-06-01',
        },
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=45) as resp:
            raw = resp.read().decode('utf-8')
        parsed = json.loads(raw)
        text = (((parsed.get('content') or [{}])[0]).get('text') or '').strip()
        text = _coerce_ai_json_text(text)
        return jsonify({'text': text, 'raw': parsed})
    except Exception as exc:
        return jsonify({'error': f'po extraction failed: {exc}'}), 500


def _coerce_ai_json_text(text: str) -> str:
    source = (text or '').strip()
    if not source:
        return source
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", source, re.IGNORECASE)
    if fenced:
        candidate = fenced.group(1).strip()
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            pass
    start = source.find('{')
    end = source.rfind('}')
    if start != -1 and end != -1 and end > start:
        candidate = source[start:end + 1].strip()
        try:
            json.loads(candidate)
            return candidate
        except Exception:
            pass
    return source


def _call_standalone_aop(fn_name: str):
    module = _load_aop_module()
    if not module:
        return jsonify({'error': 'standalone aop module not found'}), 500
    return getattr(module, fn_name)()


@app.route('/api/aop/import-sales-xlsx', methods=['POST'])
def import_sales_aop_xlsx_route():
    return _call_standalone_aop('import_sales_aop_xlsx')


@app.route('/api/kra/users', methods=['GET'])
def kra_users_route():
    return _call_standalone_aop('kra_users')


@app.route('/api/aop/import-audit', methods=['GET'])
def aop_import_audit_route():
    return _call_standalone_aop('aop_import_audit')


@app.route('/api/kra/config', methods=['GET'])
def get_kra_config_route():
    return _call_standalone_aop('get_kra_config')


@app.route('/api/kra/scorecard', methods=['GET'])
def kra_scorecard_route():
    return _call_standalone_aop('kra_scorecard')


@app.route('/api/kra/leaderboard', methods=['GET'])
def kra_leaderboard_route():
    return _call_standalone_aop('kra_leaderboard')


@app.route('/api/kra/report.csv', methods=['GET'])
def kra_report_csv_route():
    return _call_standalone_aop('kra_report_csv')


@app.route('/api/presales/learning', methods=['POST'])
def add_presales_learning_route():
    return _call_standalone_aop('add_presales_learning')


@app.route('/api/presales/feedback', methods=['POST'])
def add_presales_feedback_route():
    return _call_standalone_aop('add_presales_feedback')


@app.route('/api/presales/innovation', methods=['POST'])
def add_presales_innovation_route():
    return _call_standalone_aop('add_presales_innovation')


@app.route('/api/kra/manual-metric', methods=['POST'])
def upsert_manual_metric_route():
    return _call_standalone_aop('upsert_manual_metric')


if __name__ == "__main__":
    init_db()
    port = int(os.environ.get("PORT", "8001"))
    print(f"Simple CRM backend running on port {port}")
    print("DB host:", urlparse(DATABASE_URL).hostname)
    app.run(host="0.0.0.0", port=port, debug=True)
