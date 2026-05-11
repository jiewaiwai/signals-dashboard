import hashlib
import os
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity


st.set_page_config(page_title="CSF Signals Search", layout="wide")

APP_DIR = Path(__file__).resolve().parent
# Works whether this file sits at the repo root or inside an app/ folder.
REPO_ROOT = APP_DIR if (APP_DIR / "data").exists() else APP_DIR.parent
CSV_PATH = REPO_ROOT / "data/processed/processed_signals.csv"
VOTES_PATH = REPO_ROOT / "data/processed/signal_votes.csv"
IMAGE_BASE_URL = os.getenv("CSF_IMAGE_BASE_URL", "").rstrip("/")


# -----------------------------
# Helpers
# -----------------------------
def pick_column(df, candidates):
    lowered = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        key = cand.lower().strip()
        if key in lowered:
            return lowered[key]
    return None



def safe_text(val):
    if pd.isna(val):
        return "NA"
    text = str(val).strip()
    return text if text else "NA"



def normalize_text(val):
    if pd.isna(val):
        return ""
    return str(val).strip()



def extract_hashtags(text, lower=True):
    if not text or pd.isna(text):
        return []
    tags = re.findall(r"#[A-Za-z0-9_\-/]+", str(text))
    return [t.lower() if lower else t for t in tags]



def build_search_text(row, cols):
    parts = []
    for c in cols:
        if c and c in row.index:
            v = normalize_text(row[c])
            if v and v != "NA":
                parts.append(v)
    return " | ".join(parts)



def resolve_local_file(candidate: str):
    if not candidate or candidate == "NA":
        return None

    path = Path(candidate)
    probes = []
    if path.is_absolute():
        probes.append(path)
    else:
        probes.extend([
            Path.cwd() / path,
            REPO_ROOT / path,
            APP_DIR / path,
        ])

    for probe in probes:
        if probe.exists():
            return probe
    return None



def get_image_candidates(row, col_link, col_image):
    candidates = []

    if col_image and col_image in row.index:
        image_val = safe_text(row[col_image])
        if image_val != "NA":
            candidates.append(image_val)
            if IMAGE_BASE_URL and not image_val.lower().startswith(("http://", "https://")):
                candidates.append(f"{IMAGE_BASE_URL}/{image_val.lstrip('./')}")

    if col_link and col_link in row.index:
        link_val = safe_text(row[col_link])
        if link_val != "NA":
            candidates.append(link_val)

    return candidates



def show_image_from_candidates(candidates):
    shown = False
    for cand in candidates:
        resolved = resolve_local_file(cand)
        if resolved:
            st.image(str(resolved), use_container_width=True)
            shown = True
            break

        if cand.lower().startswith(("http://", "https://")) and any(
            cand.lower().split("?")[0].endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".webp", ".gif"]
        ):
            st.image(cand, use_container_width=True)
            shown = True
            break

    return shown


# -----------------------------
# Human review / voting helpers
# -----------------------------
VOTE_COLUMNS = ["signal_id", "vote", "comment", "timestamp"]


def widget_key(prefix, value):
    """Make short, stable Streamlit widget keys from long IDs/URLs."""
    digest = hashlib.sha1(str(value).encode("utf-8")).hexdigest()[:12]
    return f"{prefix}_{digest}"


def load_votes():
    if not VOTES_PATH.exists():
        return pd.DataFrame(columns=VOTE_COLUMNS)

    try:
        votes = pd.read_csv(VOTES_PATH)
    except Exception:
        return pd.DataFrame(columns=VOTE_COLUMNS)

    for col in VOTE_COLUMNS:
        if col not in votes.columns:
            votes[col] = ""
    return votes[VOTE_COLUMNS]


def save_vote(signal_id, vote, comment=""):
    VOTES_PATH.parent.mkdir(parents=True, exist_ok=True)
    votes = load_votes()

    new_vote = pd.DataFrame([
        {
            "signal_id": str(signal_id),
            "vote": vote,
            "comment": str(comment or "").strip(),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    ])

    votes = pd.concat([votes, new_vote], ignore_index=True)
    votes.to_csv(VOTES_PATH, index=False)


def vote_summary():
    votes = load_votes()
    if votes.empty:
        return pd.DataFrame(columns=["signal_id", "upvotes", "downvotes", "notes", "score"])

    votes["signal_id"] = votes["signal_id"].astype(str)
    vote_counts = votes.pivot_table(
        index="signal_id",
        columns="vote",
        aggfunc="size",
        fill_value=0,
    ).reset_index()

    for col in ["up", "down", "note"]:
        if col not in vote_counts.columns:
            vote_counts[col] = 0

    vote_counts["upvotes"] = vote_counts["up"].astype(int)
    vote_counts["downvotes"] = vote_counts["down"].astype(int)
    vote_counts["notes"] = vote_counts["note"].astype(int)
    vote_counts["score"] = vote_counts["upvotes"] - vote_counts["downvotes"]

    return vote_counts[["signal_id", "upvotes", "downvotes", "notes", "score"]]


@st.cache_data
def load_data():
    if not CSV_PATH.exists():
        return None
    return pd.read_csv(CSV_PATH)


@st.cache_resource
def load_embedding_model():
    return SentenceTransformer("all-MiniLM-L6-v2")


@st.cache_data(show_spinner=False)
def compute_embeddings(texts):
    model = load_embedding_model()
    return model.encode(texts, show_progress_bar=False)



def add_cluster_labels(df, embedding_matrix, n_clusters=8):
    if len(df) < 3:
        df["cluster_id"] = "Cluster 1"
        return df

    k = min(n_clusters, len(df))
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(embedding_matrix)
    df["cluster_id"] = [f"Cluster {i+1}" for i in labels]
    return df



def label_clusters(df, col_tags, col_header):
    cluster_names = {}
    for cluster in sorted(df["cluster_id"].unique()):
        subset = df[df["cluster_id"] == cluster]

        tag_counter = Counter()
        if col_tags:
            for val in subset[col_tags].fillna(""):
                tag_counter.update(extract_hashtags(val))

        if tag_counter:
            top_tags = [t for t, _ in tag_counter.most_common(2)]
            cluster_names[cluster] = " / ".join(top_tags)
        else:
            headers = subset[col_header].fillna("").astype(str).tolist() if col_header else []
            label = headers[0][:50] if headers else cluster
            cluster_names[cluster] = label

    df["cluster_label"] = df["cluster_id"].map(cluster_names)
    return df





def flatten_tags(series):
    counter = Counter()
    for val in series.fillna(""):
        counter.update(extract_hashtags(val))
    return counter


# -----------------------------
# Load data
# -----------------------------
df = load_data()
if df is None:
    st.error(f"Could not find {CSV_PATH}")
    st.stop()

# Flexible column matching
col_id = pick_column(df, ["signal_id", "record_id"])
col_time = pick_column(df, ["message_time", "time of message"])
col_type = pick_column(df, ["asset_type", "asset type"])
col_link = pick_column(df, ["final_url", "link_url", "link/image"])
col_image = pick_column(df, ["image_path", "image file", "local_image_path"])
col_header = pick_column(df, ["scraped_header", "scraped header of the link"])
col_channel = pick_column(df, ["sub_channel_name", "sub channel name"])
col_summary = pick_column(df, ["article_summary", "llm_summary", "llm summary of the article"])
col_tags = pick_column(df, ["signal_hashtags", "article_hashtags", "llm_key_hashtags"])
col_discussion_tags = pick_column(df, ["discussion_hashtags"])
col_extracted = pick_column(df, ["article_text_extracted", "article_text_extracted?"])
col_stage = pick_column(df, ["signal_stage", "suggested_stage", "stage"])
col_domain = pick_column(df, ["source_domain"])
col_fetch_status = pick_column(df, ["fetch_status"])
col_tag_origin = pick_column(df, ["tag_origin"])
col_tag_review = pick_column(df, ["tag_review_status"])

if col_type is None:
    df["asset_type"] = "link"
    col_type = "asset_type"

if col_stage is None:
    df["signal_stage"] = "NA"
    col_stage = "signal_stage"

# Merge human review scores into the main dataset.
votes_df = vote_summary()
if col_id:
    df[col_id] = df[col_id].astype(str)
    df = df.merge(votes_df, left_on=col_id, right_on="signal_id", how="left", suffixes=("", "_votes"))
else:
    df["upvotes"] = 0
    df["downvotes"] = 0
    df["notes"] = 0
    df["score"] = 0

for review_col in ["upvotes", "downvotes", "notes", "score"]:
    if review_col not in df.columns:
        df[review_col] = 0
    df[review_col] = df[review_col].fillna(0).astype(int)

search_cols = [col_header, col_summary, col_tags, col_discussion_tags, col_channel, col_domain]
df["search_text"] = df.apply(lambda row: build_search_text(row, search_cols), axis=1)

if col_tags:
    df["parsed_hashtags"] = df[col_tags].fillna("").apply(extract_hashtags)
else:
    df["parsed_hashtags"] = [[] for _ in range(len(df))]

all_tags = sorted(set(tag for tags in df["parsed_hashtags"] for tag in tags))

df["message_dt"] = pd.to_datetime(df[col_time], errors="coerce") if col_time else pd.NaT

with st.spinner("Preparing semantic search and clusters..."):
    embeddings = compute_embeddings(df["search_text"].fillna("").tolist())
    df = add_cluster_labels(df, embeddings, n_clusters=8)
    df = label_clusters(df, col_tags, col_header)

st.title("CSF Signals Search")
explore_tab, overview_tab = st.tabs(["Explore", "Overview"])

with explore_tab:
    with st.sidebar:
        st.header("Filters")

        asset_types = sorted(df[col_type].dropna().astype(str).unique().tolist())
        selected_types = st.multiselect("Asset type", asset_types, default=asset_types)

        if col_channel:
            channels = sorted(df[col_channel].dropna().astype(str).unique().tolist())
            selected_channels = st.multiselect("Sub channel", channels, default=channels)
        else:
            selected_channels = None

        keyword_search = st.text_input("Keyword search", "")
        semantic_query = st.text_input("Semantic search", "")

        sort_by = st.selectbox(
            "Sort results by",
            [
                "Newest first",
                "Semantic relevance",
                "Highest human score",
                "Most upvoted",
                "Most downvoted",
            ],
        )

        top_n = 30

    filtered = df.copy()
    filtered = filtered[filtered[col_type].astype(str).isin(selected_types)]

    if col_channel and selected_channels is not None:
        filtered = filtered[filtered[col_channel].astype(str).isin(selected_channels)]


    if keyword_search:
        filtered = filtered[
            filtered["search_text"].astype(str).str.contains(keyword_search, case=False, na=False)
        ]


    if semantic_query.strip():
        query_embedding = compute_embeddings([semantic_query])[0]
        sims = cosine_similarity([query_embedding], embeddings)[0]
        df["semantic_score"] = sims
    else:
        df["semantic_score"] = np.nan

    if sort_by == "Semantic relevance" and semantic_query.strip():
        filtered = filtered.loc[df.loc[filtered.index, "semantic_score"].sort_values(ascending=False).index]
    elif sort_by == "Highest human score":
        sort_cols = ["score"] + (["message_dt"] if "message_dt" in filtered.columns else [])
        filtered = filtered.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    elif sort_by == "Most upvoted":
        sort_cols = ["upvotes"] + (["message_dt"] if "message_dt" in filtered.columns else [])
        filtered = filtered.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    elif sort_by == "Most downvoted":
        sort_cols = ["downvotes"] + (["message_dt"] if "message_dt" in filtered.columns else [])
        filtered = filtered.sort_values(sort_cols, ascending=[False] * len(sort_cols))
    elif col_time and "message_dt" in filtered.columns:
        filtered = filtered.sort_values("message_dt", ascending=False, na_position="last")

    filtered = filtered.head(top_n)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Visible records", len(filtered))
    c2.metric("Total records", len(df))
    c3.metric("Total unique hashtags", len(all_tags))
    c4.metric("Human votes", int(df["upvotes"].sum() + df["downvotes"].sum()))

    st.markdown("### Top hashtags in current view")
    current_tag_counter = Counter(tag for tags in filtered["parsed_hashtags"] for tag in tags)
    if current_tag_counter:
        top_tags_text = "  ".join([f"`{tag}` ({count})" for tag, count in current_tag_counter.most_common(15)])
        st.markdown(top_tags_text)
    else:
        st.write("No hashtags in current view.")


    st.markdown("### Clusters in current view")
    cluster_counts = (
        filtered["cluster_label"]
        .value_counts()
        .rename_axis("cluster")
        .reset_index(name="count")
    )
    st.dataframe(cluster_counts, use_container_width=True, hide_index=True)

    st.markdown("## Results")
    for idx, row in filtered.iterrows():
        with st.container():
            left, right = st.columns([1.2, 2.8])

            with left:
                asset_type = safe_text(row.get(col_type))
                shown = show_image_from_candidates(get_image_candidates(row, col_link, col_image))
                if not shown:
                    if asset_type.lower() == "image":
                        st.write("Image not found in deployment")
                    elif col_link:
                        link = safe_text(row.get(col_link))
                        if link != "NA":
                            st.markdown(f"[Open link]({link})")

            with right:
                header = safe_text(row.get(col_header)) if col_header else "NA"
                st.subheader(header)

                meta = []
                if col_time:
                    meta.append(f"**Time:** {safe_text(row.get(col_time))}")
                if col_channel:
                    meta.append(f"**Channel:** {safe_text(row.get(col_channel))}")
                if col_domain:
                    meta.append(f"**Domain:** {safe_text(row.get(col_domain))}")
                if col_stage:
                    meta.append(f"**Stage:** {safe_text(row.get(col_stage))}")
                if col_extracted:
                    meta.append(f"**Article text extracted?:** {safe_text(row.get(col_extracted))}")
                if col_fetch_status:
                    meta.append(f"**Fetch:** {safe_text(row.get(col_fetch_status))}")
                meta.append(f"**Cluster:** {safe_text(row.get('cluster_label'))}")
                st.markdown(" | ".join(meta))

                if col_summary:
                    st.markdown(f"**Article summary:** {safe_text(row.get(col_summary))}")

                signal_id = safe_text(row.get(col_id)) if col_id else str(idx)
                upvotes = int(row.get("upvotes", 0))
                downvotes = int(row.get("downvotes", 0))
                notes = int(row.get("notes", 0))
                human_score = int(row.get("score", 0))

                with st.expander(
                    f"Human review: 👍 {upvotes} | 👎 {downvotes} | Notes {notes} | Score {human_score}",
                    expanded=False,
                ):
                    vote_col1, vote_col2 = st.columns(2)
                    with vote_col1:
                        if st.button("👍 Useful / emerging", key=widget_key("up", signal_id)):
                            save_vote(signal_id, "up")
                            st.success("Vote saved.")
                            st.rerun()
                    with vote_col2:
                        if st.button("👎 Not useful", key=widget_key("down", signal_id)):
                            save_vote(signal_id, "down")
                            st.warning("Vote saved.")
                            st.rerun()

                    comment = st.text_input(
                        "Optional note",
                        key=widget_key("comment", signal_id),
                        placeholder="Why is this useful, emerging, noisy, or irrelevant?",
                    )
                    if st.button("Save note", key=widget_key("note", signal_id)):
                        if comment.strip():
                            save_vote(signal_id, "note", comment)
                            st.success("Note saved.")
                            st.rerun()
                        else:
                            st.info("Write a note before saving.")

                if col_discussion_tags:
                    discussion_tags = extract_hashtags(row.get(col_discussion_tags), lower=False)
                    if discussion_tags:
                        st.markdown("**Discussion hashtags:** " + " ".join([f"`{t}`" for t in discussion_tags]))

                if col_tags:
                    parsed = extract_hashtags(row.get(col_tags), lower=False)
                    if parsed:
                        st.markdown("**Signal hashtags:** " + " ".join([f"`{t}`" for t in parsed]))
                    else:
                        st.markdown("**Signal hashtags:** NA")

                if col_tag_origin:
                    st.markdown(f"**Tag origin:** {safe_text(row.get(col_tag_origin))}")
                if col_tag_review:
                    st.markdown(f"**Tag review:** {safe_text(row.get(col_tag_review))}")

                if semantic_query.strip() and "semantic_score" in df.columns:
                    score = df.loc[idx, "semantic_score"]
                    st.markdown(f"**Semantic similarity:** {score:.3f}")

                if col_link:
                    link = safe_text(row.get(col_link))
                    if link != "NA":
                        st.markdown(f"[Open original]({link})")

            st.divider()
with overview_tab:
    st.markdown("## Overview")
    if col_time and df["message_dt"].notna().any():
        latest_time = df["message_dt"].max()
        recent_cutoff = latest_time - pd.Timedelta(days=30)
        recent = df[df["message_dt"] >= recent_cutoff].copy()
    else:
        latest_time = None
        recent = df.copy()

    m1, m2, m3, m4 = st.columns(4)
    m1.metric("All records", len(df))
    m2.metric("Last 30 days", len(recent))
    m3.metric("Needs tag review", int((df[col_tag_review] == "needs_review").sum()) if col_tag_review else 0)
    m4.metric("Human votes", int(df["upvotes"].sum() + df["downvotes"].sum()))

    if latest_time is not None:
        st.caption(f"Recent window anchored to latest record in dataset: {latest_time}")

    if "score" in df.columns and (df["upvotes"].sum() + df["downvotes"].sum()) > 0:
        st.markdown("### Highest-rated signals by human review")
        display_cols = []
        for candidate in [col_time, col_channel, col_header, col_domain, "upvotes", "downvotes", "score"]:
            if candidate and candidate in df.columns and candidate not in display_cols:
                display_cols.append(candidate)
        top_reviewed = df.sort_values(["score", "upvotes"], ascending=[False, False]).head(10)
        st.dataframe(top_reviewed[display_cols], use_container_width=True, hide_index=True)

    if col_channel and not recent.empty:
        st.markdown("### Signals by sub-channel (last 30 days)")
        channel_counts = recent[col_channel].fillna("NA").astype(str).value_counts().rename_axis("sub_channel_name").reset_index(name="count")
        st.bar_chart(channel_counts.set_index("sub_channel_name"))

    if col_tags:
        st.markdown("### Most common signal hashtags")
        tag_counts = flatten_tags(recent[col_tags] if not recent.empty else df[col_tags])
        if tag_counts:
            top_tags_df = pd.DataFrame(tag_counts.most_common(15), columns=["tag", "count"]).set_index("tag")
            st.bar_chart(top_tags_df)
        else:
            st.write("No hashtags available yet.")

    if col_domain:
        st.markdown("### Top source domains")
        domain_df = (
            recent[col_domain].fillna("NA").astype(str)
            .replace("NA", pd.NA)
            .dropna()
            .value_counts()
            .head(15)
            .rename_axis("source_domain")
            .reset_index(name="count")
        )
        if not domain_df.empty:
            st.bar_chart(domain_df.set_index("source_domain"))
        else:
            st.write("No source domains available.")

    if col_fetch_status:
        st.markdown("### Fetch status mix")
        fetch_df = (
            df[col_fetch_status].fillna("NA").astype(str)
            .value_counts()
            .rename_axis("fetch_status")
            .reset_index(name="count")
        )
        st.dataframe(fetch_df, use_container_width=True, hide_index=True)
