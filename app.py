import streamlit as st
import pandas as pd
import json
from pathlib import Path

# Set page config
st.set_page_config(page_title="Conversation Viewer", layout="wide")


# Load preprocessed data
@st.cache_data
def load_data():
    # Load the preprocessed DataFrame
    df = pd.read_parquet("processed_data/conversations.parquet")

    # Load the raw conversations
    with open("processed_data/raw_conversations.json", "r") as f:
        raw_conversations = json.load(f)

    return df, raw_conversations


# Check if preprocessed data exists
if not Path("processed_data/conversations.parquet").exists():
    st.error("Preprocessed data not found. Please run preprocess.py first.")
    st.stop()

# Load data
df, raw_conversations = load_data()

# Sidebar filters
st.sidebar.title("Filters")

# Add explanation about teacher-selected questions
st.sidebar.info(
    "Note about teacher-selected questions: Teachers can specify concepts they want to cover, "
    "which are then incorporated into the AI's question generation process. While teachers "
    "cannot directly specify the exact questions, they can influence the conversation by "
    "suggesting key concepts they want students to explore. The AI still makes the final "
    "decision on how to frame and ask the questions."
)

# Concepts Filter
concept_options = [
    "All conversations",
    "Teacher-selected questions",
    "100% AI selected questions",
]
selected_concepts = st.sidebar.selectbox("Concept Filter", concept_options)

# Apply filters
filtered_df = df.copy()
if selected_concepts == "Teacher-selected questions":
    filtered_df = filtered_df[filtered_df["has_concepts"] == True]
elif selected_concepts == "100% AI selected questions":
    filtered_df = filtered_df[filtered_df["has_concepts"] == False]

# Display conversation count
st.sidebar.metric("Filtered Conversations", len(filtered_df))

# Main content
st.title("Conversation Viewer")

# Conversation selector
selected_conversation = st.selectbox(
    "Select Conversation",
    filtered_df["conversation_id"].tolist(),
    format_func=lambda x: f"Conversation {x} - {filtered_df[filtered_df['conversation_id'] == x]['assignment_name'].iloc[0]}",
)

# Display conversation
if selected_conversation:
    conversation_data = raw_conversations[selected_conversation]
    metadata = df[df["conversation_id"] == selected_conversation].iloc[0]

    # Display student reading/essay if available in a dialog
    reading_type = (
        "Reading"
        if metadata["assignment_type"] == "Reading Responses"
        else "Student Work"
    )
    # Display conversation metadata
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write("**Assignment:**", metadata["assignment_name"])
        st.write("**Subject:**", metadata["assignment_subject"])
        st.write("**Grade:**", metadata["assignment_grade"])
        st.write("**Type:**", metadata["assignment_type"])
    with col2:
        st.write("**Student:**", metadata["student_name"])
        if isinstance(metadata["concepts"], list) and len(metadata["concepts"]) > 0:
            st.write("**Concepts:**", ", ".join(metadata["concepts"]))
    with col3:
        st.write("**Teacher:**", metadata["teacher_name"])

    st.divider()

    # Display questions and responses
    question_labels = [
        "I1: Hook Question",
        "BQ1: Recall Question",
        "P1: Probing Question (Follow-up to BQ1)",
        "BQ2: Analytical Question",
        "P2: Probing Question (Follow-up to BQ2)",
        "BQ3: Open-ended Question",
    ]
    question_descriptions = [
        "Intrigues the student about the reading",
        "Simple retrieve and recall question with a discrete answer",
        "Follow-up probing question for Question 2",
        "More challenging analytical question requiring synthesis",
        "Probing question based on the student's response to Question 4",
        "Abstract, open-ended question to stimulate creativity",
    ]

    # Create tabs for Questions and Student Work/Reading
    tab1, tab2 = st.tabs(["Questions", f"{reading_type}"])

    with tab1:
        for i, question in enumerate(conversation_data["questions"], 1):
            with st.expander(f"{question_labels[i-1]}", expanded=False):
                st.info(f"**Question Type:** {question_descriptions[i-1]}")
                if (
                    "conversation_flow" in conversation_data.get("assignment", {})
                    and len(conversation_data["assignment"]["conversation_flow"])
                    > i - 1
                ):
                    concept = conversation_data["assignment"]["conversation_flow"][
                        i - 1
                    ].get("concept")
                    if concept:
                        st.warning(
                            "**Teacher asked AI to include this concept:** " + concept
                        )
                st.write("**Question:**")
                st.write(question["question"])
                st.write("**Student Response:**")
                st.write(question["response"])
                if "improved_response" in question and question["improved_response"]:
                    st.write("**Improved Response (GPT-improved):**")
                    st.write(question["improved_response"])

    with tab2:
        if pd.notna(metadata["student_reading"]) and metadata["student_reading"]:
            st.markdown(metadata["student_reading"])
        else:
            st.write("No student work/reading available for this conversation.")

else:
    st.write("Please select a conversation to view.")
