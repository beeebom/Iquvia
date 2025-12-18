import streamlit as st
import tempfile
import os
from pathlib import Path
from converter import QlikConverter

st.set_page_config(page_title="Qlik to PySpark Converter", page_icon="🔄", layout="wide")

st.title("🔄 Qlik to PySpark Converter")
st.markdown("Upload your Qlik Data Load Script and get PySpark code + Semantic Model")

st.divider()

uploaded_file = st.file_uploader("Upload Qlik Script (.qvs or .txt)", type=['qvs', 'txt'])

if uploaded_file is not None:
    qlik_script = uploaded_file.read().decode('utf-8')
    
    st.success(f"✓ Loaded: {uploaded_file.name} ({len(qlik_script)} characters)")
    
    with st.expander("📄 View Input Script", expanded=False):
        st.code(qlik_script, language='sql')
    
    if st.button("🚀 Convert to PySpark", type="primary"):
        with st.spinner("Converting..."):
            try:
                converter = QlikConverter()
                pyspark_code, semantic_model = converter.convert(qlik_script)
                
                st.success("✓ Conversion complete!")
                
                col1, col2 = st.columns(2)
                
                with col1:
                    st.subheader("📊 PySpark Code")
                    st.code(pyspark_code, language='python')
                    st.download_button(
                        label="⬇️ Download PySpark Code",
                        data=pyspark_code,
                        file_name=f"{Path(uploaded_file.name).stem}_pyspark.py",
                        mime="text/plain"
                    )
                
                with col2:
                    st.subheader("📋 Semantic Model")
                    import json
                    semantic_json = json.dumps(semantic_model, indent=2)
                    st.code(semantic_json, language='json')
                    st.download_button(
                        label="⬇️ Download Semantic JSON",
                        data=semantic_json,
                        file_name=f"{Path(uploaded_file.name).stem}_semantic.json",
                        mime="application/json"
                    )
                
                st.divider()
                
                st.subheader("📈 Conversion Summary")
                col_a, col_b, col_c = st.columns(3)
                with col_a:
                    st.metric("Tables", len(semantic_model['model']['tables']))
                with col_b:
                    st.metric("Relationships", len(semantic_model['model']['relationships']))
                with col_c:
                    total_measures = sum(len(t.get('measures', [])) for t in semantic_model['model']['tables'])
                    st.metric("Measures", total_measures)
                
                with st.expander("📋 Table Details"):
                    for table in semantic_model['model']['tables']:
                        st.write(f"**{table['name']}**: {len(table['columns'])} columns")
                
            except Exception as e:
                st.error(f"❌ Conversion failed: {str(e)}")
                st.exception(e)

else:
    st.info("👆 Upload a Qlik script file (.qvs or .txt) to get started")
    
    with st.expander("ℹ️ What this converter does"):
        st.markdown("""
        **Supported Features:**
        - ✅ Variables (LET/SET) with `$(var)` expansion
        - ✅ External LOAD (FROM files)
        - ✅ Resident LOAD
        - ✅ INLINE LOAD
        - ✅ MAPPING LOAD
        - ✅ JOINs (LEFT, RIGHT, INNER)
        - ✅ WHERE filters
        - ✅ GROUP BY aggregations
        - ✅ DISTINCT
        - ✅ Qlik functions (Year, Month, Upper, Sum, Count, etc.)
        
        **Output:**
        - PySpark DataFrame code (Microsoft Fabric compatible)
        - Semantic Model JSON (Power BI compatible)
        """)

st.divider()
st.caption("Qlik-to-PySpark Converter v1.0")

