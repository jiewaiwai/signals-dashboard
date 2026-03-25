import os
from pathlib import Path

import pandas as pd
import streamlit as st


st.set_page_config(page_title="CSF Signals", layout="wide")


CSV_PATH = Path("processed_signals.csv")


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


def split_hashtags(text):
    if not text or text == "NA":
        return []
    parts = str(text).replace(",", " ").split()
    return [p for p in parts if p.startswith("#")]


if not CSV_PATH.exists():
    st.error(f"Could not find {CSV_PATH}")
    st.stop()

df = pd.read_csv(CSV_PATH)

# Flexible column matching
col_time = pick_column(df, ["message_time", "time of message"])
col_sender = pick_column(df, ["sender", "person who sent the message"])
col_type = pick_column(df, ["asset_type", "asset type"])
col_link = pick_column(df, ["link_url", "link/image"])
col_image = pick_column(df, ["image_path"])
col_desc = pick_column(df, ["person_description", "person's description"])
col_header = pick_column(df, ["scraped_header", "scraped header of the link"])
col_channel = pick_column(df, ["sub_channel_name", "sub channel name"])
col_summary = pick_column(df, ["article_summary", "llm_summary", "LLM summary of the article"])
col_tags = pick_column(df, ["article_hashtags", "llm_key_hashtags", "LLM summary of key areas of the article using hashtags"])
col_extracted = pick_column(df, ["article_text_extracted", "article_text_extracted?", "article_text_extracted_yes"])

if col_type is None:
    df["asset_type"] = "link"
    col_type = "asset_type"

st.title("CSF Signals Local Search")

with st.sidebar:
    st.header("Filters")

    asset_types = sorted(df[col_type].dropna().astype(str).unique().tolist())
    selected_types = st.multiselect("Asset type", asset_types, default=asset_types)

    if col_channel:
        channels = sorted(df[col_channel].dropna().astype(str).unique().tolist())
        selected_channels = st.multiselect("Sub channel", channels, default=channels)
    else:
        selected_channels = None

    if col_sender:
        senders = sorted(df[col_sender].dropna().astype(str).unique().tolist())
        selected_senders = st.multiselect("Sender", senders, default=senders)
    else:
        selected_senders = None

    search_text = st.text_input("Keyword search", "")

    tag_filter = st.text_input("Hashtag contains", "")

    if col_extracted:
        extracted_vals = sorted(df[col_extracted].dropna().astype(str).unique().tolist())
        selected_extracted = st.multiselect("Article text extracted?", extracted_vals, default=extracted_vals)
    else:
        selected_extracted = None

filtered = df.copy()

filtered = filtered[filtered[col_type].astype(str).isin(selected_types)]

if col_channel and selected_channels is not None:
    filtered = filtered[filtered[col_channel].astype(str).isin(selected_channels)]

if col_sender and selected_senders is not None:
    filtered = filtered[filtered[col_sender].astype(str).isin(selected_senders)]

if col_extracted and selected_extracted is not None:
    filtered = filtered[filtered[col_extracted].astype(str).isin(selected_extracted)]

if search_text:
    mask = pd.Series(False, index=filtered.index)
    for col in [col_desc, col_header, col_summary, col_tags, col_link]:
        if col and col in filtered.columns:
            mask = mask | filtered[col].astype(str).str.contains(search_text, case=False, na=False)
    filtered = filtered[mask]

if tag_filter and col_tags:
    filtered = filtered[filtered[col_tags].astype(str).str.contains(tag_filter, case=False, na=False)]

st.caption(f"{len(filtered)} records")

for _, row in filtered.iterrows():
    with st.container():
        c1, c2 = st.columns([1, 3])

        with c1:
            asset_type = safe_text(row.get(col_type))
            if asset_type == "image" and col_image:
                image_path = safe_text(row.get(col_image))
                if image_path != "NA" and os.path.exists(image_path):
                    st.image(image_path, use_container_width=True)
                else:
                    st.write("Image not found")
            elif col_link:
                link = safe_text(row.get(col_link))
                if link != "NA":
                    st.markdown(f"[Open link]({link})")

        with c2:
            header = safe_text(row.get(col_header)) if col_header else "NA"
            st.subheader(header)

            meta = []
            if col_time:
                meta.append(f"**Time:** {safe_text(row.get(col_time))}")
            if col_sender:
                meta.append(f"**Sender:** {safe_text(row.get(col_sender))}")
            if col_channel:
                meta.append(f"**Channel:** {safe_text(row.get(col_channel))}")
            if col_extracted:
                meta.append(f"**Article text extracted?:** {safe_text(row.get(col_extracted))}")

            st.markdown(" | ".join(meta))

            if col_desc:
                st.markdown(f"**Person description:** {safe_text(row.get(col_desc))}")

            if col_summary:
                st.markdown(f"**Article summary:** {safe_text(row.get(col_summary))}")

            if col_tags:
                tags = safe_text(row.get(col_tags))
                st.markdown(f"**Hashtags:** {tags}")

        st.divider()
