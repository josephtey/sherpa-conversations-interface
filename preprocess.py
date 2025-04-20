import pandas as pd
import json
from pathlib import Path
import random
from collections import Counter


def has_valid_concepts(conversation):
    """Check if conversation has non-empty concepts."""
    concepts = conversation.get("assignment", {}).get("concepts", [])
    return bool(concepts) and all(concepts)  # Check if list exists and no empty strings


def get_student_reading(conversation):
    """Extract student reading/essay based on assignment type."""
    assignment = conversation.get("assignment", {})

    # If it's a reading response, get text from assignment
    if assignment.get("assignment_type") == "Reading Responses":
        return assignment.get("text")

    # Otherwise, get from student_work in conversation
    return conversation.get("student_work")


def preprocess_data(base_conversations=500):
    """
    Preprocess conversation data and save to disk.

    Args:
        base_conversations (int): Number of base conversations to process (will be split equally between with/without concepts).
            Additional conversations from the most popular assignment will be included on top of this.
    """
    # Create data directory if it doesn't exist
    Path("processed_data").mkdir(exist_ok=True)

    # Load original JSON data
    try:
        with open("conversations.json", "r") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print("Attempting to fix the JSON file...")
        with open("conversations.json", "r") as f:
            content = f.read()
        # Remove any trailing commas in arrays or objects
        content = content.replace(",]", "]").replace(",}", "}")
        # Attempt to load the fixed JSON
        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            print(f"Unable to fix JSON file. Error: {e}")
            return

    # Find most popular assignment first
    assignment_counts = Counter(conv["assignment"]["_id"] for conv in data)
    most_popular_assignment_id, popular_count = assignment_counts.most_common(1)[0]

    # Get all conversations from the most popular assignment
    popular_assignment_conversations = [
        conv for conv in data if conv["assignment"]["_id"] == most_popular_assignment_id
    ]

    # Remove popular assignment conversations from main dataset to avoid duplicates
    remaining_conversations = [
        conv for conv in data if conv["assignment"]["_id"] != most_popular_assignment_id
    ]

    # Split remaining conversations by concept presence
    with_concepts = [
        conv for conv in remaining_conversations if has_valid_concepts(conv)
    ]
    without_concepts = [
        conv for conv in remaining_conversations if not has_valid_concepts(conv)
    ]

    print(f"\nInitial counts:")
    print(f"Found {len(with_concepts)} conversations with concepts")
    print(f"Found {len(without_concepts)} conversations without concepts")
    print(
        f"Found {len(popular_assignment_conversations)} conversations in most popular assignment"
    )

    # Sample equal amounts from remaining conversations
    half = base_conversations // 2  # This will be 250 for base_conversations=500
    with_concepts = random.sample(with_concepts, min(half, len(with_concepts)))
    without_concepts = random.sample(without_concepts, min(half, len(without_concepts)))

    # Combine all conversations we want to keep
    sampled_conversations = (
        with_concepts + without_concepts + popular_assignment_conversations
    )

    print(f"\nFinal sampling:")
    print(f"- Sampled {len(with_concepts)} conversations with concepts")
    print(f"- Sampled {len(without_concepts)} conversations without concepts")
    print(
        f"- Included {len(popular_assignment_conversations)} conversations from popular assignment"
    )
    print(f"Total conversations to process: {len(sampled_conversations)}")

    # Process conversations into DataFrame format
    conversations = []
    raw_conversations = {}  # Store raw data separately
    popular_assignment_data = {}  # Store popular assignment data separately

    for conv in sampled_conversations:
        assignment = conv.get("assignment", {})
        student = conv.get("student", {})
        teacher = conv.get("teacher", {})

        # Get student reading/essay
        student_reading = get_student_reading(conv)

        conv_id = conv.get("_id")
        record = {
            "conversation_id": conv_id,
            "assignment_id": assignment.get("_id"),
            "assignment_name": assignment.get("title"),
            "assignment_type": assignment.get("assignment_type"),
            "assignment_subject": assignment.get("subject"),
            "assignment_grade": assignment.get("grade"),
            "has_concepts": has_valid_concepts(conv),
            "concepts": assignment.get("concepts", []),
            "student_id": student.get("_id"),
            "student_name": student.get("name"),
            "teacher_id": teacher.get("_id"),
            "teacher_name": teacher.get("name"),
            "student_reading": student_reading,
            "is_popular_assignment": conv["assignment"]["_id"]
            == most_popular_assignment_id,
        }
        conversations.append(record)
        raw_conversations[conv_id] = conv

        # Store popular assignment data separately
        if conv["assignment"]["_id"] == most_popular_assignment_id:
            popular_assignment_data[conv_id] = conv

    # Convert to DataFrame and save as parquet
    df = pd.DataFrame(conversations)
    df.to_parquet("processed_data/conversations.parquet")

    # Save raw conversations as JSON
    with open("processed_data/raw_conversations.json", "w") as f:
        json.dump(raw_conversations, f)

    # Save popular assignment data separately
    with open("processed_data/popular_assignment.json", "w") as f:
        json.dump(
            {
                "assignment_id": most_popular_assignment_id,
                "assignment_name": popular_assignment_conversations[0]["assignment"][
                    "title"
                ],
                "conversations": popular_assignment_data,
            },
            f,
        )

    print(f"\nData breakdown:")
    print(
        f"- Most popular assignment: {popular_assignment_conversations[0]['assignment']['title']}"
    )
    print(
        f"  - Type: {popular_assignment_conversations[0]['assignment']['assignment_type']}"
    )
    print(
        f"  - Subject: {popular_assignment_conversations[0]['assignment']['subject']}"
    )
    print(f"  - Grade: {popular_assignment_conversations[0]['assignment']['grade']}")

    # Print storage info
    processed_data_dir = Path("processed_data")
    print(f"\nStorage info:")
    print(
        f"- conversations.parquet: {(processed_data_dir / 'conversations.parquet').stat().st_size / 1024:.1f} KB"
    )
    print(
        f"- raw_conversations.json: {(processed_data_dir / 'raw_conversations.json').stat().st_size / 1024:.1f} KB"
    )
    print(
        f"- popular_assignment.json: {(processed_data_dir / 'popular_assignment.json').stat().st_size / 1024:.1f} KB"
    )


if __name__ == "__main__":
    preprocess_data(base_conversations=500)  # Always process 500 base conversations
