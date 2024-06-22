from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
import json

app = FastAPI()

def carregar_registros():
    with open("json_for_case.json", "r", encoding="utf-8") as file:
        registros = json.load(file)
    return registros

def salvar_registros(registros):
    with open("json_for_case.json", "w", encoding="utf-8") as file:
        json.dump(registros, file, indent=4, ensure_ascii=False)

@app.get("/read/")
async def root(registro_id: int = None):
    registros = carregar_registros()
    if registro_id is not None:
        for registro in registros:
            if registro["id"] == registro_id:
                return JSONResponse(content=registro)
        raise HTTPException(status_code=404, detail="Registro não encontrado")
    else:
        return JSONResponse(content=registros)

@app.post("/upsert/")
async def adicionar_registro(request: Request, novo_registro: dict):
    registros = carregar_registros()
    ids_existem = [registro["id"] for registro in registros]
    if novo_registro["id"] in ids_existem:
        for registro in registros:
            if registro["id"] == novo_registro["id"]:
                registro.update(novo_registro)
                salvar_registros(registros)
                return JSONResponse(content={"message": "Registro atualizado"})
    else:
        registros.append(novo_registro)
        salvar_registros(registros)
        return JSONResponse(content={"message": "Registro adicionado"})

@app.delete("/delete/")
async def deletar_registro(registro_id: int):
    registros = carregar_registros()
    registro_encontrado = None
    for registro in registros:
        if registro["id"] == registro_id:
            registro_encontrado = registro
            break

    if registro_encontrado:
        registros.remove(registro_encontrado)
        salvar_registros(registros)
        return JSONResponse(content={"message": "Registro deletado"})
    else:
        raise HTTPException(status_code=404, detail="Registro não encontrado")