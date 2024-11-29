FROM python:3.9

WORKDIR /streamlit

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY dashboard.py .

EXPOSE 8501

CMD ["streamlit", "run", "dashboard.py"]