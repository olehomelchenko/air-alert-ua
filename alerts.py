import json
import pandas as pd

with open("alerts.json", "r") as f:
    data = json.load(f)

# process the raw json structure to extract needed data
input_data = [
    [
        e["id"],
        e["date"],
        e["date_unixtime"],
        e.get("edited"),
        e.get("edited_unixtime"),
        "|".join(
            [
                ee.get("text")
                for ee in e["text"]
                if type(ee) == dict and ee["type"] == "bold"
            ]
        ),
        "|".join(
            [
                ee.get("text")
                for ee in e["text"]
                if type(ee) == dict and ee["type"] == "plain"
            ]
        ),
        ",".join(
            [
                ee.get("text")
                for ee in e["text"]
                if type(ee) == dict and ee["type"] == "hashtag"
            ]
        ),
        "".join([ee for ee in e["text"] if type(ee) == str]).strip(),
    ]
    for e in data["messages"]
    if e["type"] == "message"
]

# transform to dataframe
df_orig = pd.DataFrame(
    input_data,
    columns=[
        "id",
        "date",
        "date_unixtime",
        "edited",
        "edited_unixtime",
        "text_bold",
        "text_plain",
        "text_hashtag",
        "text_string",
    ],
)

# transform columns to appropriate formats
df = df_orig.copy()
for c in ["date", "edited"]:
    df[c] = pd.to_datetime(df[c])

# split by months and rewrite.
# TODO: rewrite the loop so that it only adds missing message IDs
for yearmonth in df["date"].dt.strftime("%Y-%m").unique():
    df_to_write = df[df["date"].dt.strftime("%Y-%m") == yearmonth].reset_index(
        drop=True
    )
    df_to_write.to_csv(f"csv/{yearmonth}.csv", index=False)
