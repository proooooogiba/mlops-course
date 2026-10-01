import numpy as np
import streamlit as st

from app import bootstrap
from src.adapters.csv_input import read_transactions
from src.adapters.messages import score_message, transaction_message
from src.application.use_cases import SubmissionFailed

st.set_page_config(page_title="Fraud Detection", page_icon="🛡️", layout="wide")
st.title("Скоринг транзакций")
st.caption("Загрузите транзакции и посмотрите результаты обработки.")
if bootstrap.preprocessing_mode() == "demo":
    st.warning("Демонстрационный препроцессинг: модель применяется, но качество на данных соревнования не проверено.")

upload_tab, results_tab = st.tabs(["Отправить транзакции", "Результаты"])
with upload_tab:
    uploaded = st.file_uploader("CSV в формате test.csv", type="csv")
    st.download_button("Скачать test.csv", data=bootstrap.test_csv(), file_name="test.csv", mime="text/csv")
    if uploaded is not None:
        try:
            transactions = read_transactions(uploaded.getvalue())
            st.write(f"Готово к отправке: {len(transactions)} транзакций")
            st.dataframe([transaction_message(tx) for tx in transactions[:10]], use_container_width=True)
            if st.button("Отправить на скоринг", type="primary"):
                progress = st.progress(0)
                try:
                    with bootstrap.build_submit_transactions() as use_case:
                        sent = use_case.execute(transactions, on_progress=lambda count: progress.progress(count / len(transactions)))
                except SubmissionFailed as exc:
                    st.error(f"Отправлено {exc.sent} из {len(transactions)}. Повторите отправку; идентификаторы сохранятся.")
                except Exception:
                    st.error("Не удалось отправить транзакции. Проверьте доступность сервиса и повторите попытку.")
                else:
                    st.success(f"Отправлено {sent} транзакций. Откройте «Результаты» и нажмите «Посмотреть результаты».")
        except Exception as exc:
            st.error(f"Не удалось обработать файл: {exc}")

with results_tab:
    if st.button("Посмотреть результаты", type="primary"):
        try:
            st.session_state["results"] = bootstrap.build_get_results().execute()
        except Exception:
            st.error("Не удалось получить результаты. Проверьте доступность базы и повторите попытку.")
    if "results" in st.session_state:
        snapshot = st.session_state["results"]
        st.metric("Транзакций обработано", snapshot.total)
        st.subheader("10 последних транзакций с флагом фрода")
        if not snapshot.fraud_transactions:
            st.info("Транзакций с fraud_flag = 1 пока нет.")
        else:
            rows = [{**score_message(saved.result), "received_at": saved.received_at} for saved in snapshot.fraud_transactions]
            st.dataframe(rows, hide_index=True, use_container_width=True)
        st.subheader("Распределение скоров последних 100 транзакций")
        if not snapshot.recent_scores:
            st.info("Результатов пока нет. Сначала отправьте транзакции.")
        else:
            counts, edges = np.histogram(snapshot.recent_scores, bins=np.linspace(0, 1, 21))
            st.bar_chart({"Интервал скора": [f"{lo:.2f}–{hi:.2f}" for lo, hi in zip(edges[:-1], edges[1:])],
                          "Количество": counts}, x="Интервал скора", y="Количество")
            st.caption(f"В гистограмме {len(snapshot.recent_scores)} транзакций. Нажмите кнопку ещё раз для обновления.")
