import os
import tempfile

# import cv2
import numpy as np
import streamlit as st

from src.components.inferance import Prediction_Pipeline
from pathlib import Path
from dotenv import load_dotenv
load_dotenv()

MODEL_PATH = Path("aws_model") / "best_chest_xray_model.keras"
MODEL_KEY = "best_chest_xray_model.keras"

# def build_overlay(original_img: np.ndarray, mask: np.ndarray) -> np.ndarray:
#     mask_binary = (mask > 0).astype(np.uint8)
#     # Using a slightly more clinical "Cyan" overlay instead of pure Red
#     overlay_layer = np.zeros_like(original_img)
#     overlay_layer[:, :, 1] = mask_binary * 255  # Green Channel
#     overlay_layer[:, :, 2] = mask_binary * 200  # Hint of Blue
#     return cv2.addWeighted(original_img, 0.8, overlay_layer, 0.4, 0)

# from PIL import Image

def build_overlay(original_img: np.ndarray, mask: np.ndarray) -> np.ndarray:
    mask_binary = (mask > 0).astype(np.uint8)

    # Create cyan overlay
    overlay = np.zeros_like(original_img)
    overlay[:, :, 1] = mask_binary * 255  # Green
    overlay[:, :, 2] = mask_binary * 200  # Blue

    # Manual blending (instead of cv2.addWeighted)
    blended = (original_img * 0.8 + overlay * 0.4).clip(0, 255).astype(np.uint8)

    return blended


@st.cache_resource
def load_pipeline(model_path_init: str) -> Prediction_Pipeline:
    return Prediction_Pipeline(model_path=model_path_init)


def main() -> None:
    st.set_page_config(
        page_title="PneumoScan AI | Diagnostics",
        page_icon="🫁",
        layout="wide",
    )

    # --- UNIQUE GLASSMORPHIC CSS ---
    st.markdown(
        """
        <style>
            /* Main Background */
            .stApp {
                background: linear-gradient(135deg, #0f172a 0%, #1e293b 100%);
                color: #f8fafc;
            }
            
            /* Glassmorphic Cards */
            div[data-testid="stMetric"] {
                background: rgba(255, 255, 255, 0.05);
                border: 1px solid rgba(255, 255, 255, 0.1);
                backdrop-filter: blur(10px);
                padding: 20px;
                border-radius: 20px;
                box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.3);
            }
            
            /* Custom Header */
            .main-title {
                background: linear-gradient(90deg, #38bdf8, #818cf8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                font-size: 3rem;
                font-weight: 850;
                letter-spacing: -1px;
                margin-bottom: 0px;
            }
            
            .sub-title {
                color: #94a3b8;
                font-size: 1.1rem;
                margin-bottom: 2rem;
                text-transform: uppercase;
                letter-spacing: 2px;
            }

            /* Buttons */
            .stButton>button {
                border-radius: 12px;
                background: linear-gradient(90deg, #0284c7, #4f46e5);
                border: none;
                color: white;
                font-weight: 600;
                padding: 0.6rem 2rem;
                transition: all 0.3s ease;
            }
            
            .stButton>button:hover {
                transform: translateY(-2px);
                box-shadow: 0 10px 20px rgba(0,0,0,0.4);
            }

            /* Image styling */
            img {
                border-radius: 15px;
                border: 1px solid rgba(255, 255, 255, 0.1);
            }
            
            /* Slider & Uploader */
            .stSlider [data-baseweb="slider"] {
                margin-top: 20px;
            }
            
            /* Sidebar Styling */
            section[data-testid="stSidebar"] {
                background-color: rgba(15, 23, 42, 0.8);
                border-right: 1px solid rgba(255, 255, 255, 0.1);
            }
        </style>
    """,
        unsafe_allow_html=True,
    )

    # --- HEADER SECTION ---
    st.markdown(
        '<div class="main-title">PNEUMOSCAN<span style="color:#f8fafc">.AI</span></div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="sub-title">Advanced Thoracic Pathology Segmentation</div>',
        unsafe_allow_html=True,
    )

    ## if load model in clould or local
    # if not os.path.exists(MODEL_PATH):
    #     st.error(f"Critical Error: Neural weights not found at `{MODEL_PATH}`")
    #     st.stop()

    pipeline = load_pipeline(model_path_init=str(MODEL_PATH))

    # --- SIDEBAR CONTROLS ---
    with st.sidebar:
        st.markdown("### 🛠️ Configuration")
        threshold = st.slider(
            "Confidence Sensitivity",
            0.05,
            0.95,
            0.20,
            0.05,
            help="Adjusting the sensitivity changes the detection strictness.",
        )
        st.markdown("---")
        st.markdown("### 📂 Data Source")
        uploaded_file = st.file_uploader(
            "Load Chest Radiograph", type=["png", "jpg", "jpeg"]
        )
        st.markdown("---")
        st.caption("v2.4.1 | Clinical Decision Support Tool")

    # --- MAIN CONTENT ---
    if uploaded_file is None:
        st.info(
            "System Ready. Please upload a chest X-ray in the sidebar to begin analysis."
        )
        return

    # Trigger Analysis
    col_btn1, col_btn2 = st.columns([1, 2])
    with col_btn1:
        run_inference = st.button("START RADIOLOGICAL SCAN", use_container_width=True)

    if not run_inference:
        st.image(uploaded_file, caption="Input Preview", width=500)
    else:
        file_suffix = os.path.splitext(uploaded_file.name)[1] or ".png"
        temp_path = None

        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=file_suffix) as tmp:
                tmp.write(uploaded_file.getbuffer())
                temp_path = tmp.name

            with st.spinner("🧠 AI is analyzing pulmonary regions..."):
                original_img, mask, detection_img = pipeline.predict(
                    temp_path, threshold=threshold
                )

            # Calculation
            mask_binary = (mask > 0).astype(np.uint8)
            affected_area_pct = 100.0 * mask_binary.sum() / mask_binary.size
            overlay_img = build_overlay(original_img, mask_binary)

            # --- RESULTS DASHBOARD ---
            st.markdown("### 📊 Diagnostic Metrics")
            m_col1, m_col2, m_col3 = st.columns(3)

            with m_col1:
                st.metric("Area of Involvement", f"{affected_area_pct:.2f}%")
            with m_col2:
                status = "DETECTION CONFIRMED" if affected_area_pct > 0 else "NEGATIVE"
                st.metric("Detection Status", status)
            with m_col3:
                # Simulated confidence for UI appeal
                confidence = np.max(mask) if affected_area_pct > 0 else 0
                st.metric("Model Certainty", "HIGH" if affected_area_pct > 0 else "N/A")

            st.markdown("---")

            # Visual Tabs for clean UI
            tab1, tab2 = st.tabs(["🎯 ANALYSIS VIEW", "🔍 SEGMENTATION VIEW"])

            with tab1:
                t1_col1, t1_col2 = st.columns(2)
                with t1_col1:
                    st.image(
                        detection_img,
                        caption="AI Bounding Box",
                        use_container_width=True,
                    )
                with t1_col2:
                    st.image(
                        overlay_img,
                        caption="Pathology Heatmap Overlay",
                        use_container_width=True,
                    )

            with tab2:
                t2_col1, t2_col2 = st.columns(2)
                with t2_col1:
                    st.image(
                        original_img, caption="Original X-ray", use_container_width=True
                    )
                with t2_col2:
                    st.image(
                        (mask_binary * 255),
                        caption="Raw AI Binary Mask",
                        use_container_width=True,
                    )

        except Exception as e:
            st.error(f"Pipeline Error: {e}")
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)


if __name__ == "__main__":
    main()

## streamlit run app.py
