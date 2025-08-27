import streamlit as st 
import pandas as pd
import random, os
from gensim.models import KeyedVectors
from datetime import datetime

# ============= SIDEBAR =============
pages = ["Play","Cheat"]
languages = ['Français','English']
with st.sidebar:
    language = st.selectbox("Choose language", options=languages)
    st.title("Summary")
    page = st.radio("Navigated", pages, label_visibility="collapsed")
# ===================================

# ============= OPTIONS =============
CACHE_DIR = "temp_data"
os.makedirs(CACHE_DIR, exist_ok=True)

def get_frWac_path():
    if "frWac_path" not in st.session_state:
        combined_path = os.path.join(CACHE_DIR, "frWac_no_postag_phrase_500_cbow_cut10_stripped.bin")
        if not os.path.exists(combined_path):
            # Reconstruire le fichier à partir des deux parties
            with open(combined_path, "wb") as outfile:
                for part in ["data/frWac_no_postag_phrase_500_cbow_cut10_stripped.bin.part001",
                             "data/frWac_no_postag_phrase_500_cbow_cut10_stripped.bin.part002"]:
                    with open(part, "rb") as f:
                        outfile.write(f.read())
        st.session_state.frWac_path = combined_path
    return st.session_state.frWac_path

if language == "Français": model_path = get_frWac_path()
elif language == "English": model_path = './data/GoogleNews-vectors-negative300_stripped.bin'
model = KeyedVectors.load_word2vec_format(model_path, binary=True, unicode_errors='ignore')
# =================================== 

# ============ FUNCTION =============
def select_new_word_to_guess():
    word_answer = random.choice(list(model.key_to_index.keys()))
    word_answer = "appartenance"
    list_topn = model.most_similar(word_answer, topn=1000)
    dico_topn = {word: {'degree': round(score*100, 2),'index': 999-i} for i, (word, score) in enumerate(list_topn)}
    return word_answer , dico_topn

def guess_a_word(word_guess, word_answer, dico_topn, word_fail):
    if word_guess not in list(model.key_to_index.keys()):
        st.write("Not a good word")
        return
    if word_guess in word_fail:
        st.write("already guess")
        return
    if word_guess == word_answer :
        st.write("\u200B")
        return {'N°':len(word_fail)+1, 'degree': 100.00,'index': "1000"}
    elif word_guess in dico_topn:
        st.write("\u200B")
        return {'N°':len(word_fail)+1} | dico_topn[word_guess]
    else:
        st.write("\u200B")
        return {'N°':len(word_fail)+1, 'degree': round(model.similarity(word_guess, word_answer)*100,2),'index': ""}

if "word_answer" not in st.session_state:
    st.session_state.word_answer, st.session_state.dico_topn = select_new_word_to_guess()
    st.session_state.word_fail = {}
# =================================== 

# ============ PAGE PLAY ============
if page == pages[0]:

    # Create entry and button 
    col1, col2, space = st.columns([1, 1, 3])
    with col1: input = st.text_input("", key="guess_input", label_visibility="collapsed")
    with col2: submit = st.button("Valider")

    # Put the proposition in lowercase
    word_guess = input.lower()

    # When user press enter or click the button
    if st.session_state.get("guess_input") or submit:
        option_of_word_guess = guess_a_word(word_guess, st.session_state.word_answer, st.session_state.dico_topn, st.session_state.word_fail)
        if option_of_word_guess is not None:
            st.session_state.word_fail[word_guess] =  {"word":word_guess} | option_of_word_guess

    if st.session_state.word_fail != {}:
        df = pd.DataFrame(st.session_state.word_fail).T
        df.set_index('N°', inplace=True)
        st.table(df.sort_values(by=['degree'],ascending=False))

if page == pages[1]:
    pass


# input = st.text_input("text", key="text")

# def clear_text():
#     st.session_state["text"] = ""
    
# st.button("clear text input", on_click=clear_text)
# st.write(input)