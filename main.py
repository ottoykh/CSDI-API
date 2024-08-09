from fastapi import FastAPI, Query
from typing import List, Dict, Union
from urllib.parse import unquote
import jieba

jieba.load_userdict("Ref/number.txt")
jieba.load_userdict("Ref/area.txt")
jieba.load_userdict("Ref/placename.txt")
jieba.load_userdict("Ref/Street_csdi.txt")
jieba.load_userdict("Ref/Street_data.txt")
jieba.load_userdict("Ref/Building_nt.txt")
jieba.load_userdict("Ref/Building_kh.txt")

app = FastAPI()

# Predefined areas, districts, and sub-districts
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

def segment_input(input_str: str) -> List[Dict[str, Union[str, List[str]]]]:
    decoded_input = unquote(input_str)  # Decode URL-encoded input string
    results = []

    # Iterate through areas and districts, using a single loop for efficiency
    for area, districts in areas.items():
        for district, sub_districts in districts.items():
            # Check for sub-district match and extract relevant parts
            match = next((sd for sd in sub_districts if sd in decoded_input), None)
            if match:
                building_street = decoded_input.replace(match, '').strip()
                building_street = building_street.lstrip(area).lstrip(district).lstrip(match).strip()
                
                # Split into street and building details
                street, building_details = (building_street.split('號', 1) + [''])[:2]
                street = street.strip() + '號'
                results.append({
                    'area': area,
                    'district': district,
                    'sub_district': match,
                    'street': [street],
                    'building': building_details.strip()
                })
                return results

    if decoded_input.strip():
        street, building_details = (decoded_input.strip().split('號', 1) + [''])[:2]
        street = street.strip() + '號'
        results.append({
            'area': '',
            'district': '',
            'sub_district': '',
            'street': [street],
            'building': building_details.strip()
        })

    return results

@app.get("/seg")
async def segment_input_text(q: str = Query(..., description="The input text to be segmented")):
    try:
        segmented_output = segment_input(q)
        return {"segmented_text": segmented_output}
    except Exception as e:
        return {"error": str(e)}
