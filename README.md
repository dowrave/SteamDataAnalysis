# steamspypi 데이터 수집 및 분석 프로젝트

### 파일 설명
- `MySQL` `host`, `user`, `password`, `port`, 데이터베이스 이름 2개(`steam, steam_raw`)를 `mysql_info.py`에 저장해야 함
1. `CollectData.py` : `steamspy`에 있는 보유자 수 기준 상위 5000 + a개의 데이터 수집.
2. `SqlToCsv.py` : `MySQL`에 있는 테이블에서 3~4개의 테이블을 `csv`로 변환함.
3. `CollectReview.ipynb` : `steam API`를 이용해 스팀에 있는 리뷰 데이터를 수집.
  - 영어, 한국어, 일본어를 수집했으며 각각 최대 5000개까지만 수집.
4. `review.ipynb` : 어떤 게임의 리뷰 데이터를 시각화함. 워드클라우드로 할 예정.
  - "패키지가 유효하지 않다"는 오류 때문에 가상환경에서는 스크립트가 잘 작동하지 않음.
  - 다행히 **전역환경에서는 잘 작동**해서, 코드만 복붙함.

### 테이블 설명
1. `info` : 게임에 관한 변하지 않는 정보를 저장
2. `time_value` : 매일 수집한 데이터 정보 저장
3. `lang_genre` : 게임이 지원하는 언어와 장르를 저장. `SqlToCsv.py`에 의해 csv 전환 시 원핫인코딩됨
4. `tag` : 유저가 투표해서 결정하는 게임의 장르. 종류가 너무 많아서 일단 JSON으로 저장.

### 사용 라이브러리 & 프로그램
- `requirements.txt`에 있음
  - `Jpype1, konlpy`를 이용할 경우, `Jpype1 == 1.4.0`으로 명시되어 있지만 작동하지 않을 수 있음
  - 이 경우 `Lib/site-packages`에 있는 `JPype1-1.4.0-cp39-cp39-win_amd64.whl` 파일을 pip로 설치하자.

- `Java, MySQL` 사전 설치 & 세팅 필요
  - `Java`는 `review.py`에서만 필요

## 프로젝트 내용

#### steamspy에서 데이터 수집
- `steamspy`에서는 기간에 따른 데이터를 제공하지 않고, 1일치의 데이터만 보여줌 -> 따라서 하루에 1번 데이터를 수집한다면 기간에 따른 데이터의 변화를 살필 수 있음
- 데이터는 4개의 테이블로 보관되며, 그 중 `info` 테이블을 이용해 순위에서 벗어난 데이터도 계속적으로 추적하여 데이터를 수집함
- 일단 수집한 상태의 데이터를 `steam_raw` 데이터베이스에 저장한 뒤, 이를 가공해 4개의 테이블로 분리하여 `steam` DB에 저장된다.


### 자동화
- 컨테이너를 띄우는 데 사용된 스크립트는 `Container` 폴더에 저장됨.
- `docker compose`를 이용해 2개의 컨테이너를 띄웠음
  - `steamspy-collector` : 파이썬 스크립트가 있으며 `steamspy`에서 데이터를 수집해 `steamspy-mysql` 컨테이너에 저장함. 스크립트는 `schedule` 라이브러리를 이용, 6시간에 1번씩 실행됨
  - `steamspy-mysql` : `mysql` 컨테이너로, 수집된 데이터를 저장하는 역할. 호스트 OS에선 `localhost:33060` 으로, `steamspy-collector`에선 `steamspy-mysql:3306`으로 접근할 수 있다.

### 리뷰 데이터 수집
- 스팀에서도 API를 지원하며, `appid`만 있으면 리뷰 데이터에 접근할 수 있음
- 한글 리뷰 데이터를 수집해서 `steam_raw` DB의 `raw_review` 테이블에 저장했음.
  - 수집 시간이 매우 오래 걸림(최소 12시간 이상)
  - 한 게임에 리뷰가 1000개를 넘어가는 경우가 잘 없어서, 영어 리뷰 데이터라도 수집해야 할까 고민 중

#### [프로젝트 일지](https://github.com/dowrave/TIL/tree/main/Obsidian/1.%20Projects/%EC%8A%A4%ED%8C%80%20%EB%8D%B0%EC%9D%B4%ED%84%B0%20%EB%B6%84%EC%84%9D/%EC%9D%BC%EC%A7%80)