import streamlit as st 
import pandas as pd
import random
from gensim.models import KeyedVectors

# ============= SIDEBAR =============
pages = ["Play","Cheat"]
languages = ["Français","English"]
with st.sidebar:
    language = st.selectbox("Choose language", options=languages)
    st.title("Summary")
    page = st.radio("Navigated", pages, label_visibility="collapsed")
# ===================================

# ============= OPTIONS =============
if language == "Français": model_path = "./data/frWac_no_postag_phrase_500_cbow_cut10_stripped.bin"
elif language == "English": model_path = "./data/GoogleNews-vectors-negative300_stripped.bin"
model = KeyedVectors.load_word2vec_format(model_path, binary=True, unicode_errors="ignore")
# =================================== 

# ============ FUNCTION =============
def select_new_word_to_guess():
    word_answer = random.choice(list(model.key_to_index.keys()))
    word_answer = "appartenance"
    list_topn = model.most_similar(word_answer, topn=1000)
    dico_topn = {word: {"degree": score*100,"index": 999-i} for i, (word, score) in enumerate(list_topn)}
    return word_answer , dico_topn

def guess_a_word(word_guess, word_answer, dico_topn, word_fail):
    if word_guess not in list(model.key_to_index.keys()):
        st.write("Not a good word")
        return
    if any(entry["word"] == word_guess for entry in word_fail.values()):
        st.write("already guessed")
        return
    if word_guess == word_answer :
        st.write("\u200B")
        return {"word": word_guess, "degree": 100.00,"index": "1000"}
    elif word_guess in dico_topn:
        st.write("\u200B")
        return {"word": word_guess} | dico_topn[word_guess]
    else:
        st.write("\u200B")
        return {"word": word_guess, "degree": model.similarity(word_guess, word_answer)*100,"index": ""}

def clear_guess_input():
    st.session_state["guess_input"] = ""
# ============ FUNCTION =============

# ============== INIT ===============
if "word_answer" not in st.session_state:
    st.session_state.word_answer, st.session_state.dico_topn = select_new_word_to_guess()
    st.session_state.word_fail = {}
# =================================== 

# ============ PAGE PLAY ============
if page == pages[0]:
    dico_topn = st.session_state.dico_topn
    word_answer = st.session_state.word_answer
    word_fail = st.session_state.word_fail
    col1, col2, space = st.columns([1, 1, 3])
    with col1:st.text_input("guess input", key="guess_input", label_visibility="collapsed", on_change=clear_guess_input)
    with col2:submit = st.button("Valider")

    word_guess = st.session_state["guess_input"].lower()
    if st.session_state.get("guess_input") or submit:
        option_of_word_guess = guess_a_word(word_guess, word_answer, dico_topn, word_fail)
        if option_of_word_guess is not None:
            word_fail[len(word_fail)+1] = option_of_word_guess
    if word_fail != {}:
        pd.options.display.float_format = '{:.2f}'.format
        df = pd.DataFrame(word_fail).T
        df["degree"] = df["degree"].apply(lambda x: f"{x:.2f}")
        st.table(df.sort_values(by="degree", ascending=False))

if page == pages[1]:
    pass
