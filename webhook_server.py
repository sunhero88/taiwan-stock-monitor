# webhook_server.py
from flask import Flask, request, jsonify
import datetime
import traceback

app = Flask(__name__)

@app.route('/trigger', methods=['GET', 'POST'])
def trigger_analysis():
    try:
        # 優先從 GET 參數取（瀏覽器測試用）
        if request.method == 'GET':
            command = request.args.get('command', '盤後報告')
            market = request.args.get('market', 'tw')

        # 如果是 POST，優先從 JSON 取（未來我呼叫時用）
        elif request.method == 'POST':
            if request.is_json:
                data = request.json
                command = data.get('command', '盤後報告')
                market = data.get('market', 'tw')
            else:
                command = request.form.get('command', '盤後報告')
                market = request.form.get('market', 'tw')

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # 模擬生成報告（未來這裡可呼叫 analyzer.run(market)）
        report = f"""
V12.3 智能分析報告 - {command}
時間：{timestamp}
市場：{market.upper()}

**大盤總覽**：加權指數假設收盤 30,500 點（漲 1.2%），成交 6,800 億。

**三大法人**：
- 外資買超 85 億
- 投信買超 12 億
- 自營商買超 45 億

**主流族群**：
- 電子零組件 +2.8%
- 半導體 +2.1%
- 電腦週邊 +1.5%

**操作建議**：
續抱台積電、緯創，注意紅旗。

**紅旗警示**：
- 毛利率稀釋風險（N2製程）
- 應收帳款回收天數惡化
        """

        return jsonify({
            "status": "success",
            "command": command,
            "market": market,
            "report": report,
            "timestamp": timestamp
        })

    except Exception as e:
        # 錯誤處理，回傳詳細訊息
        error_msg = traceback.format_exc()
        return jsonify({
            "status": "error",
            "message": str(e),
            "traceback": error_msg
        }), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)