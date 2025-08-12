# Sistema de Acompanhamento de Obras (MVP)

Aplicativo simples em **Python + Streamlit** para registrar andamento dos serviços e visualizar progresso (funciona no celular).
Inclui **Obra Berlin / Etapa Reboco** com 599 lotes × 15 serviços importados.

## Instalação (Windows)
1) Instale **Python 3.10+**: https://www.python.org/downloads/
2) Extraia este zip para uma pasta, ex.: `C:\obra_app`
3) No Prompt de Comando:
```
cd C:\obra_app
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
streamlit run app.py
```
4) Abra o link no navegador. No celular (mesma rede), use o IP do PC, ex.: `http://SEU.IP:8501`.

## Uso
- **Lançamentos**: selecione Obra, Etapa, Serviço, Lote, Status, Datas, Observações e (opcional) Foto → Salvar.
- **Dashboard**: totais (Não iniciado, Em execução, Concluído).
- **Previsto × Executado**: visão por lote/serviço com exportação Excel.

## Observações
- Banco **SQLite** em `db.sqlite3` (já inicializado).
- Uploads de fotos vão para a pasta `uploads/`.