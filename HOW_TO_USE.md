# 🎓 Beginner's Guide: How to Convert Qlik Scripts

## 📋 What You Need
- Your Qlik script file (`.qvs` or `.txt`)
- This project installed on your computer
- PowerShell or Command Prompt

---

## 🚀 Step-by-Step Instructions

### **STEP 1: Place Your Qlik Script File**

Put your `.qvs` file anywhere you want. For beginners, I recommend putting it directly in the project folder:

```
C:\Users\satya\OneDrive\Desktop\iquvia\
```

**Example:** Copy your file here and rename it to `my_script.qvs`

---

### **STEP 2: Open PowerShell**

1. Press `Windows Key + R`
2. Type: `powershell`
3. Click OK

---

### **STEP 3: Go to Project Folder**

Copy and paste this into PowerShell:
```powershell
cd C:\Users\satya\OneDrive\Desktop\iquvia
```
Press `Enter`

---

### **STEP 4: Activate Virtual Environment**

Copy and paste this:
```powershell
venv\Scripts\activate
```
Press `Enter`

You should see `(venv)` appear before your prompt.

---

### **STEP 5: Convert Your File**

Run the converter with this command:

```powershell
python convert_qlik.py YOUR_FILE_NAME.qvs
```

**Replace `YOUR_FILE_NAME.qvs` with your actual filename!**

---

## 📝 Real Examples

### Example 1: File in Project Folder
**Your file:** `customer_data.qvs` (located in `C:\Users\satya\OneDrive\Desktop\iquvia\`)

**Command:**
```powershell
python convert_qlik.py customer_data.qvs
```

**Output generated:**
- ✅ `customer_data_output.py` - Your PySpark code
- ✅ `customer_data_semantic.json` - Your semantic model

**Location:** Same folder as your input file

---

### Example 2: File on Desktop
**Your file:** `sales_etl.qvs` (located on Desktop)

**Command:**
```powershell
python convert_qlik.py C:\Users\satya\Desktop\sales_etl.qvs
```

**Output generated:**
- ✅ `C:\Users\satya\Desktop\sales_etl_output.py`
- ✅ `C:\Users\satya\Desktop\sales_etl_semantic.json`

---

### Example 3: Custom Output Name
**Your file:** `data_load.qvs`

**Command:**
```powershell
python convert_qlik.py data_load.qvs final_output.py
```

**Output generated:**
- ✅ `final_output.py`
- ✅ `final_output_semantic.json`

---

### Example 4: Test with Sample File
**To practice first, use the included example:**

**Command:**
```powershell
python convert_qlik.py examples\sample_script.qvs
```

**Output generated:**
- ✅ `examples\sample_script_output.py`
- ✅ `examples\sample_script_semantic.json`

---

## 🎯 Quick Reference Card

```
┌─────────────────────────────────────────────────────────┐
│  QUICK COMMAND TEMPLATE                                 │
├─────────────────────────────────────────────────────────┤
│  1. Open PowerShell                                     │
│  2. cd C:\Users\satya\OneDrive\Desktop\iquvia           │
│  3. venv\Scripts\activate                               │
│  4. python convert_qlik.py YOUR_FILE.qvs                │
└─────────────────────────────────────────────────────────┘
```

---

## 📂 Where Are My Output Files?

**Rule:** Output files are created in the **SAME FOLDER** as your input file.

| If your input is here... | Your output will be here... |
|--------------------------|----------------------------|
| `C:\Users\satya\Desktop\script.qvs` | `C:\Users\satya\Desktop\script_output.py` |
| `C:\Projects\iquvia\data.qvs` | `C:\Projects\iquvia\data_output.py` |
| `examples\sample_script.qvs` | `examples\sample_script_output.py` |

---

## ✅ What You'll See When It Works

```
Reading Qlik script from: my_script.qvs
Parsing Qlik script...
  Parsed 5 statements
Transforming to internal representation...
  Created 3 table(s)
  Execution order: ['Customers', 'Orders', 'Summary']
Generating PySpark code...
Generating semantic model...

PySpark code saved to: my_script_output.py
Semantic model saved to: my_script_semantic.json

============================================================
CONVERSION COMPLETED SUCCESSFULLY!
============================================================
```

---

## ❌ Common Mistakes

### Mistake 1: Forgot to activate venv
**Error:** `No module named 'app'`

**Fix:** Run `venv\Scripts\activate` first

---

### Mistake 2: Wrong file path
**Error:** `File not found: my_script.qvs`

**Fix:** 
- Check your file name spelling
- Use full path: `C:\Users\satya\Desktop\my_script.qvs`

---

### Mistake 3: Not in project folder
**Error:** `convert_qlik.py not found`

**Fix:** Navigate to project folder first:
```powershell
cd C:\Users\satya\OneDrive\Desktop\iquvia
```

---

## 🆘 Need Help?

If something doesn't work:
1. Make sure you're in the correct folder
2. Make sure venv is activated (you see `(venv)` in PowerShell)
3. Check your file name and path are correct
4. Make sure your `.qvs` file is a valid Qlik script

---

## 🎉 That's It!

You don't need to modify ANY code. Just:
1. Put your file somewhere
2. Run the command with your filename
3. Get your output!

**It's that simple!**

