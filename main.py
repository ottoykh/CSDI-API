from fastapi import FastAPI, Query
from typing import Optional
import jieba
import pandas as pd

jieba.load_userdict("Ref/number.txt")
jieba.load_userdict("Ref/area.txt")
jieba.load_userdict("Ref/placename.txt")
jieba.load_userdict("Ref/Street_csdi.txt")
jieba.load_userdict("Ref/Street_data.txt")
jieba.load_userdict("Ref/Building_nt.txt")
jieba.load_userdict("Ref/Building_kh.txt")

app = FastAPI()

def segment_text(text: str):
    lines = text.splitlines()
    segmented_lines = []
    for line in lines:
        seg_list = jieba.lcut(line, cut_all=False)
        segmented_lines.append(seg_list)

    return segmented_lines


@app.get("/sa")
async def segment_input_text(q: str = Query(..., description="The input text to be segmented")):
    try:
        segmented_output = segment_text(q)
        return {"segmented_text": segmented_output}

    except Exception as e:
        return {"error": str(e)}
