import os
import re
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st
from sentence_transformers import SentenceTransformer
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import cosine_similarity


st.set_page_config(page_title="CSF Signals Search", layout="wide")

APP_DIR = Path(__file__).resolve().parent
REPO_ROOT = APP_DIR.parent
CSV_PATH = REPO_ROOT / "data/processed/processed_signals.csv"
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



def find_top_cooccurring_tags(df, selected_tag, col_tags, top_n=10):
    counter = Counter()
    if not col_tags or not selected_tag:
        return []

    for val in df[col_tags].fillna(""):
        tags = set(extract_hashtags(val))
        if selected_tag in tags:
            tags.discard(selected_tag)
            counter.update(tags)

    return counter.most_common(top_n)



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

        if col_domain:
            domains = sorted([d for d in df[col_domain].dropna().astype(str).unique().tolist() if d])
            selected_domains = st.multiselect("Source domain", domains, default=domains)
        else:
            selected_domains = None

        if col_fetch_status:
            statuses = sorted(df[col_fetch_status].dropna().astype(str).unique().tolist())
            selected_statuses = st.multiselect("Fetch status", statuses, default=statuses)
        else:
            selected_statuses = None

        if col_tag_review:
            review_states = sorted(df[col_tag_review].dropna().astype(str).unique().tolist())
            selected_review_states = st.multiselect("Tag review status", review_states, default=review_states)
        else:
            selected_review_states = None

        if col_stage:
            stages = sorted(df[col_stage].dropna().astype(str).unique().tolist())
            selected_stages = st.multiselect("Signal stage", stages, default=stages)
        else:
            selected_stages = None

        only_signals = st.checkbox("Only weak signals / emerging patterns")

        keyword_search = st.text_input("Keyword search", "")
        semantic_query = st.text_input("Semantic search", "")

        selected_tag = st.selectbox("Hashtag focus", ["(none)"] + all_tags)

        cluster_options = ["(all)"] + sorted(df["cluster_label"].dropna().unique().tolist())
        selected_cluster = st.selectbox("Cluster", cluster_options)

        if col_extracted:
            extracted_vals = sorted(df[col_extracted].dropna().astype(str).unique().tolist())
            selected_extracted = st.multiselect("Article text extracted?", extracted_vals, default=extracted_vals)
        else:
            selected_extracted = None

        top_n = st.slider("Max results", 5, 100, 30)

    filtered = df.copy()
    filtered = filtered[filtered[col_type].astype(str).isin(selected_types)]

    if col_channel and selected_channels is not None:
        filtered = filtered[filtered[col_channel].astype(str).isin(selected_channels)]

    if col_domain and selected_domains is not None and len(selected_domains) > 0:
        filtered = filtered[filtered[col_domain].astype(str).isin(selected_domains)]

    if col_fetch_status and selected_statuses is not None:
        filtered = filtered[filtered[col_fetch_status].astype(str).isin(selected_statuses)]

    if col_tag_review and selected_review_states is not None:
        filtered = filtered[filtered[col_tag_review].astype(str).isin(selected_review_states)]

    if col_stage and selected_stages is not None:
        filtered = filtered[filtered[col_stage].astype(str).isin(selected_stages)]

    if only_signals and col_stage:
        filtered = filtered[
            filtered[col_stage].astype(str).str.lower().isin(["weak signal", "emerging pattern"])
        ]

    if col_extracted and selected_extracted is not None:
        filtered = filtered[filtered[col_extracted].astype(str).isin(selected_extracted)]

    if keyword_search:
        filtered = filtered[
            filtered["search_text"].astype(str).str.contains(keyword_search, case=False, na=False)
        ]

    if selected_tag != "(none)":
        filtered = filtered[filtered["parsed_hashtags"].apply(lambda tags: selected_tag in tags)]

    if selected_cluster != "(all)":
        filtered = filtered[filtered["cluster_label"] == selected_cluster]

    if semantic_query.strip():
        query_embedding = compute_embeddings([semantic_query])[0]
        sims = cosine_similarity([query_embedding], embeddings)[0]
        df["semantic_score"] = sims
        filtered = filtered.loc[df.loc[filtered.index, "semantic_score"].sort_values(ascending=False).index]
    else:
        df["semantic_score"] = np.nan

    filtered = filtered.head(top_n)

    c1, c2, c3 = st.columns(3)
    c1.metric("Visible records", len(filtered))
    c2.metric("Total records", len(df))
    c3.metric("Total unique hashtags", len(all_tags))

    st.markdown("### Top hashtags in current view")
    current_tag_counter = Counter(tag for tags in filtered["parsed_hashtags"] for tag in tags)
    if current_tag_counter:
        top_tags_text = "  ".join([f"`{tag}` ({count})" for tag, count in current_tag_counter.most_common(15)])
        st.markdown(top_tags_text)
    else:
        st.write("No hashtags in current view.")

    if selected_tag != "(none)":
        st.markdown(f"### Co-occurring hashtags with `{selected_tag}`")
        co_tags = find_top_cooccurring_tags(filtered, selected_tag, col_tags)
        if co_tags:
            st.markdown("  ".join([f"`{tag}` ({count})" for tag, count in co_tags]))
        else:
            st.write("No co-occurring hashtags found.")

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
                        st.caption(f"Tried image_path: {safe_text(row.get(col_image))}")
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

    m1, m2, m3 = st.columns(3)
    m1.metric("All records", len(df))
    m2.metric("Last 30 days", len(recent))
    m3.metric("Needs tag review", int((df[col_tag_review] == "needs_review").sum()) if col_tag_review else 0)

    if latest_time is not None:
        st.caption(f"Recent window anchored to latest record in dataset: {latest_time}")

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
