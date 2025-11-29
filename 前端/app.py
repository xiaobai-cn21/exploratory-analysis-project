import joblib
from pathlib import Path

from flask import Flask, jsonify, render_template, request


def create_app():
    """Flask application factory."""
    app = Flask(__name__)
    
    # 加载分类模型（MLP Classifier）
    PROJECT_ROOT = Path(__file__).resolve().parent
    MODEL_PATH = PROJECT_ROOT / "static" / "models" / "best_classification_model.pkl"
    model = None
    
    if MODEL_PATH.exists():
        try:
            model = joblib.load(MODEL_PATH)
            print(f"MLP Classifier 模型已加载: {MODEL_PATH}")
        except Exception as e:
            print(f"加载模型失败: {e}")
    else:
        print(f"模型文件不存在: {MODEL_PATH}")

    @app.route("/")
    def index():
        return render_template("index.html")

    @app.route("/schools")
    def schools():
        return render_template("schools.html")

    @app.route("/equity")
    def equity():
        return render_template("equity.html")

    @app.route("/research")
    def research():
        return render_template("research3.html")

    @app.route("/research1")
    def research1():
        return render_template("research1.html")

    @app.route("/research2")
    def research2():
        return render_template("research2.html")
    
    @app.route("/api/predict_proficiency", methods=["POST"])
    def predict_proficiency():
        """预测 AP/IB 是否达标（二分类）"""
        if model is None:
            return jsonify({"error": "模型未加载，请先运行训练脚本"}), 500
        
        try:
            data = request.get_json()
            
            # 准备特征数据
            features = {
                "aggregation_index": data.get("aggregation_index", 4),
                "NRC_CODE": data.get("NRC_CODE", 3),
                "COUNTY_CODE": data.get("COUNTY_CODE", 1),
                "NYC_IND": data.get("NYC_IND", 0),
                "tested_student_cnt": data.get("tested_student_cnt", 30),
                "aggregation_type": data.get("aggregation_type", "Public School"),
                "LEA_NAME": "UNKNOWN",  # 预测时使用默认值
                "NRC_DESC": "UNKNOWN",
                "COUNTY_NAME": "UNKNOWN",
                "SUBGROUP_NAME": data.get("SUBGROUP_NAME", "All Students"),
                "APIB_IND": data.get("APIB_IND", "AP"),
                "SUBJECT_AREA": data.get("SUBJECT_AREA", "ELA"),
                "GRADE_LEVEL": data.get("GRADE_LEVEL", "11th Grade"),
            }
            
            # 转换为 DataFrame
            import pandas as pd
            import numpy as np
            df = pd.DataFrame([features])
            
            # 使用分类模型预测概率
            proba = model.predict_proba(df)[0]
            probability = float(proba[1])  # 达标（类别1）的概率
            
            # 二分类预测（阈值0.5）
            prediction = int(probability >= 0.5)
            
            return jsonify({
                "probability": probability,
                "prediction": prediction,
                "is_proficient": bool(prediction),
                "model": "MLP Classifier",
                "metrics": {
                    "auc": 0.956,
                    "accuracy": 0.89,
                    "f1_score": 0.92
                }
            })
            
        except Exception as e:
            import traceback
            return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 400

    return app


if __name__ == "__main__":
    application = create_app()
    application.run(debug=True)
