# 📊 EDA + LLM Assistant  

### 🔎 Project Overview  
A one-stop **data exploration and intelligent interpretation tool**:  
- Upload any CSV dataset  
- Automatically generate **EDA reports** (statistics, distributions, correlations, missing values)  
- Use an **LLM (OpenAI API)** to translate results into **business-friendly summaries and recommendations**  

Designed for **data scientists, business analysts, and students** to quickly explore datasets and produce insights.  

---

## 🚀 Features  
- **Automated EDA**: Generate profiling reports using `ydata-profiling` / `sweetviz`  
- **LLM-Powered Insights**: Convert statistical outputs into natural language summaries  
- **Flexible Input**: Upload any structured CSV dataset  
- **Report Export**: Save results as HTML/PDF reports (optional)  
- **Extensible**: Future integration with causal inference (GeoLift) and MMM (Robyn)  

---

## 📂 Project Structure  
```
project/
│── app.py               # Main script (Streamlit or Python)
│── requirements.txt     # Dependencies
│── README.md            # Documentation
│── sample_data.csv      # Example dataset
```

---

## ⚡ Quick Start  

### 1. Clone the repository  
```bash
git clone https://github.com/yourname/eda-llm-assistant.git
cd eda-llm-assistant
```

### 2. Install dependencies  
```bash
pip install -r requirements.txt
```

### 3. Run the demo  
```bash
python app.py
```

If using Streamlit:  
```bash
streamlit run app.py
```

---

## 📝 Example  

Input data (`geolift_output.csv`):  
```csv
geo,treatment,control,lift,ci_low,ci_high,p_value
NY,1200,1000,0.20,0.10,0.30,0.01
CA,900,850,0.06,-0.05,0.15,0.20
TX,1500,1300,0.15,0.05,0.25,0.03
```

Auto-generated LLM summary:  
```
- New York uplift +20% (95% CI: 10%–30%), statistically significant
- Texas uplift +15% (95% CI: 5%–25%), statistically significant
- California uplift not significant, CI includes 0

Recommendations:
1. Increase investments in New York and Texas
2. Reassess California campaigns
3. Run additional experiments for validation
```

---

## 🔮 Future Enhancements  
- Integrate **Robyn (Meta MMM)** outputs for automated budget optimization insights  
- Integrate **GeoLift (Meta causal inference)** for causal experiment reports  
- Add **interactive dashboard (Streamlit)** for visualization  

---

## 🤝 Contributing  
Contributions via PRs or issues are welcome!  

---

## 📜 License  
MIT License  
