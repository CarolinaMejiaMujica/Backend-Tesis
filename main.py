from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routes.index import tiempo
from routes.index import espacio
from routes.agrupamiento import agrupamiento

app = FastAPI(tittle='Herramienta analítica interactiva', description='Proyecto de Tesis',version='1.0.2')

origins = [
    "http://localhost",
    "http://localhost:8080",
    "https://localhost",
    "https://localhost:8080",
    '*',
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(espacio)
app.include_router(tiempo)
app.include_router(agrupamiento)