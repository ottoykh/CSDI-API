from fastapi import FastAPI, Query
from typing import List, Dict, Union, Optional
from urllib.parse import unquote
import jieba
import jieba.posseg as pseg

jieba.load_userdict("Ref/number.txt")
jieba.load_userdict("Ref/area.txt")
jieba.load_userdict("Ref/placename.txt")
jieba.load_userdict("Ref/Street_csdi.txt")
jieba.load_userdict("Ref/Street_data.txt")
jieba.load_userdict("Ref/Building_nt.txt")
jieba.load_userdict("Ref/Building_kh.txt")

app = FastAPI()

# Areas data for Hong Kong
areas = {
    '香港': {'中西區': ['堅尼地城', '石塘咀', '西營盤', '上環', '中環', '金鐘', '半山', '山頂'],
             '灣仔': ['灣仔', '銅鑼灣', '跑馬地', '大坑', '掃桿埔', '渣甸山'],
             '東區': ['天后', '寶馬山', '北角', '鰂魚涌', '西灣河', '筲箕灣', '柴灣', '小西灣'],
             '南區': ['薄扶林', '香港仔', '鴨脷洲', '黃竹坑', '壽臣山', '淺水灣', '舂磡角', '赤柱', '大潭', '石澳']},
    '九龍': {'油尖旺': ['尖沙咀', '油麻地', '西九龍', '京士柏', '旺角', '大角咀'],
             '深水埗': ['美孚', '荔枝角', '長沙灣', '深水埗', '石硤尾', '又一村', '大窩坪', '昂船洲'],
             '九龍城': ['紅磡', '土瓜灣', '馬頭角', '馬頭圍', '啟德', '九龍城', '何文田', '九龍塘', '筆架山'],
             '黃大仙': ['新蒲崗', '黃大仙', '東頭', '橫頭磡', '樂富', '鑽石山', '慈雲山', '牛池灣'],
             '觀塘': ['坪石', '九龍灣', '牛頭角', '佐敦谷', '觀塘', '秀茂坪', '藍田', '油塘', '鯉魚門']},
    '新界': {'葵青': ['葵涌', '青衣'],
             '荃灣': ['荃灣', '梨木樹', '汀九', '深井', '青龍頭', '馬灣', '欣澳'],
             '屯門': ['大欖涌', '掃管笏', '屯門', '藍地'],
             '元朗': ['洪水橋', '廈村', '流浮山', '天水圍', '元朗', '新田', '落馬洲', '錦田', '石崗', '八鄉'],
             '北區': ['粉嶺', '聯和墟', '上水', '石湖墟', '沙頭角', '鹿頸', '烏蛟騰'],
             '大埔': ['大埔墟', '大埔', '大埔滘', '大尾篤', '船灣', '樟木頭', '企嶺下'],
             '沙田': ['大圍', '沙田', '火炭', '馬料水', '烏溪沙', '馬鞍山'],
             '西貢': ['清水灣', '西貢', '大網仔', '將軍澳', '坑口', '調景嶺', '馬游塘'],
             '離島': ['長洲', '坪洲', '大嶼山', '東涌', '南丫島']}
}

road_related_words = ['路', '道', '街', '巷', '橋', '隧道', '大道', '高速公路', '公路', '馬路', '徑']

def segment_input(input_str: str) -> Dict[str, Union[str, List[str]]]:
    decoded_input = unquote(input_str)
    words = pseg.cut(decoded_input)
    
    result = {
        'area': '',
        'district': '',
        'sub_district': '',
        'street_name': '',
        'street_number': '',
        'building': ''
    }
    
    for word, flag in words:
        if any(road_word in word for road_word in road_related_words):
            result['street_name'] += word
        elif flag == 'm' or '號' in word:
            if '號' in word:
                result['street_number'] = word.split('號')[0] + '號'
            else:
                result['street_number'] = word
        elif word in areas.keys():
            result['area'] = word
        elif any(word in sd for sd in [item for sublist in areas.values() for district in sublist for sd in sublist[district]]):
            for area, districts in areas.items():
                for district, sub_districts in districts.items():
                    if word == district:
                        result['district'] = word
                        result['area'] = area
                    elif word in sub_districts:
                        result['sub_district'] = word
                        result['district'] = district
                        result['area'] = area

    remaining_input = decoded_input.replace(result['street_name'], '').replace(result['street_number'], '')
    result['building'] = remaining_input.strip()

    return result

@app.get("/seg")
async def segment_input_text(
    q: str = Query(..., description="The input text to be segmented"),
    item: Optional[str] = Query(None, description="Specific terms to output, options: a, d, sd, st, sn, b")
):
    try:
        segmented_output = segment_input(q)
        if item:
            # Filter the result based on the requested items
            filtered_output = {k: v for k, v in segmented_output.items() if k[0] in item}
            return {"segmented_text": filtered_output}
        return {"segmented_text": segmented_output}
    except Exception as e:
        return {"error": str(e)}

