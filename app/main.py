
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.endpoints import router
#venv\Scripts\activate
app = FastAPI(
    title="Qlik-to-PySpark Converter",
    description="Convert Qlik Data Load Scripts to PySpark code and semantic models",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1", tags=["converter"])

@app.get("/")
async def root():

    return {
        "service": "Qlik-to-PySpark Converter",
        "version": "1.0.0",
        "docs": "/docs"
    }
