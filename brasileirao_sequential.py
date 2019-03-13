"""
Thiago Alves - 13/03/19
"""


import bs4 as bs
import urllib.request
import os
from lxml import etree
import re
import time

def extract_header_round(id_header: str, page_parsed) -> bs.element.Tag:
    return page_parsed.find(attrs={"id": id_header}).parent

def extract_round_header_text(header_round: bs.element.Tag) -> str:
    spans = header_round.find_all('span')
    # to deal with accentuation issues
    if len(spans) == 5:
        return spans[0].text
    return spans[1].text 

def extract_team_data(team: bs.element.Tag, st_home: bool) -> dict:
    home_or_visitor = 'HOME' if st_home else 'VISITOR'
    team_data = {
        "team_wiki_"  + home_or_visitor: team.find_all('a')[not st_home].get('href'),
        "team_name_"  + home_or_visitor: team.find_all('a')[not st_home].get('title'),
        "team_nick_"  + home_or_visitor: team.find_all('a')[not st_home].text,
        "team_state_" + home_or_visitor: team.find_all('a')[st_home].get('title').replace(' (estado)', '')
    }
    return team_data

def extract_stadium_data(stadium: bs.element.Tag) -> dict:
    stadium_data = {
        "stadium_name": stadium.find('a').get("title"),
        "stadium_nick": stadium.find('a').text,
        "stadium_city": stadium.find_all('a')[1].text
    }
    return stadium_data

def process_match_header(header: bs.element.Tag) -> dict:
    day_month, home, result, visitor, stadium = header.find_all('td')
    
    day_month = {"day_month": day_month.text.strip()}
    home = extract_team_data(team=home, st_home=True)
    result = {"result" : result.text.strip()}
    visitor = extract_team_data(team=visitor, st_home=False)
    stadium = extract_stadium_data(stadium=stadium)
    
    return {**day_month, **home, **result, **visitor, **stadium}

def extract_goals_time(goals: bs.element.Tag, dict_name: str) -> dict:
    goals_time = {
        dict_name: str([i.text.replace("'", "").strip() for i in goals.find_all('span')])
    }
    return goals_time

def extract_more_info(more_info: bs.element.Tag) -> dict:
    all_elements = more_info.find_all('b')
    audience = [i for i in all_elements if i.text == 'Público:']
    audience = audience[0].nextSibling.strip().replace(' ', '').replace('.','') if audience else ""
    income = [i for i in all_elements if i.text == 'Renda:']
    income = income[0].nextSibling.strip().replace(',', '.') if income else ""

    info = {
        "audience": audience,
        "income": income
    }
    return info

def process_match_details(details: bs.element.Tag) -> dict:
    hour, goals_home, _, goals_visitor, more_info = details.find_all('td')
    hour = {"hour": hour.text.replace('h',':').strip()}
    goals_home_team = extract_goals_time(goals_home, "goals_home_team")
    goals_visitor_team =  extract_goals_time(goals_visitor, "goals_visitor_team")
    more_info = extract_more_info(more_info)

    return {**hour, **goals_home_team, **goals_visitor_team, **more_info }

def extract_match_data(tb_match: bs.element.Tag) -> dict:
    rows = tb_match.find_all('tr')
    if not rows:
        return {}

    header, details = rows[:2]
    
    header = process_match_header(header=header)
    details = process_match_details(details=details)
    return {**header, **details}

def extract_data_url(url: str) -> None:
    page = urllib.request.urlopen(url).read()
    page_parsed = bs.BeautifulSoup(page, 'lxml')
    
    page_description = page_parsed.find(attrs={"id": "firstHeading"}).text
    year = re.search(r'\d{4}', page_description).group()
    
    all_rounds = [i for i in page_parsed.find_all('span', {"class": "mw-headline"}) if (i.parent.name== "h3" or i.parent.name== "h2") and 'rodada' in str.lower(i.text)]
    header_round = extract_header_round(all_rounds[0].get('id'), page_parsed)
    previous = header_round
    current = header_round.find_next_sibling()

    with open('brazilian_championship.csv', 'a') as file:
        while 'Ver também' not in current.text:
            if current.name == 'h3':
                header_round = current
            elif previous.name != 'table':
                current_data = extract_match_data(current)
                if current_data:
                    line = ",".join([year, extract_round_header_text(header_round)] + list(current_data.values()))
                    file.write(line.replace('[','"[').replace(']', ']"')+'\n')        
                    print(", ".join([current_data['day_month'], year, extract_round_header_text(header_round),
                          current_data['team_nick_HOME'], current_data['result'],
                          current_data['team_nick_VISITOR']]))
            previous = current
            current = current.find_next_sibling()
            
            


if __name__ == "__main__":
    with open('urls.txt', 'r') as infile:
        urls = [i.strip() for i in infile.readlines()]
        
    OUT_HEADER = ['YEAR', 'ROUND', 'day_month', 'team_wiki_HOME', 'team_name_HOME', 'team_nick_HOME', 
                'team_state_HOME', 'result', 'team_wiki_VISITOR', 'team_name_VISITOR', 'team_nick_VISITOR',
                'team_state_VISITOR', 'stadium_name', 'stadium_nick', 'stadium_city', 'hour', 
                'goals_home_time', 'goals_visitor_time', 'audience', 'income']

    with open('brazilian_championship.csv', 'w') as file:
        file.write(",".join(OUT_HEADER)+"\n")
            
    start = time.time()
    for i in urls:
        extract_data_url(i)
    end = time.time()
    total = end-start
    print(f"\n\nRUNNING TIME: {round(total, 2)} seconds\n")
