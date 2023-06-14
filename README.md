# steamspypi 데이터 수집 및 분석 프로젝트

## 프로젝트 내용
- `steamspypi`를 이용, 보유자 수 기준 상위 5000개의 게임에 대한 데이터를 수집 및 분석

### 수집 (`230515~`)
- 수집된 데이터를 2개의 DB로 나눠서 저장함 : 수집된 원본 DB `steam_raw`와 가공한 DB `steam`
    - 가공 DB에는 4개의 테이블 : `info, time_value, lang_genre, tag` 
    - 원본 DB는 3개의 테이블 : `raw, raw_detail, checkpoint`

#### steamspy에서 데이터 수집
- `steamspy`에서는 기간에 따른 데이터를 제공하지 않고, 1일치의 데이터만 보여줌 -> 따라서 하루에 1번 데이터를 수집한다면 기간에 따른 데이터의 변화를 살필 수 있음
    - `request` : `all`과 `appdetail`이 있으며, 전자는 1분에 1000개의 데이터를 수집하지만 누락된 내용이 있고, 후자는 1초에 1개의 데이터를 수집할 수 있으며 `language, genre, tag` 등의 정보를 추가로 제공함.
    - 보유자 수 기준 상위 5000개이므로, 날짜에 따라 5000개 내에 포함된 게임이 달라질 수 있음 -> 한 번 수집된 적이 있는 데이터는 `info` 테이블의 `appid`를 이용해 계속 추적함
    - `request = all`이 정상적으로 작동하지 않는 경우가 있으며, 이 경우 `info` 테이블에 있는 `appid`을 이용해 `request = appdetails`를 보내 데이터를 수집함 : 이 경우 데이터 수집 시간은 약 1시간으로 늘어남

#### 테이블 구성
- `steam` DB에 있는 테이블만 서술함.
- `info` : `appid`, `name`, `publisher`, `developer`, `initialprice`
- `time_value`
  -  `appid`, `name`, `date`
  -  `ccu` : 하루 중 최고 동시접속자 수
  -  `positive` : 긍정 리뷰 수 
  -  `negative` : 부정 리뷰 수
  -  `average_2weeks`, `median_2weeks` : (추측) 지난 2주간 보유자 중 게임을 플레이 한 평균/중위 시간(분)
  -  `price` : 현재 가격
  -  `discount` : 할인율(0~100)
- `lang_genre`
  - `appid`, `name`
  - `langunage` : `,`로 구분되었으며 여러 언어가 하나의 특성에 들어가 있음
  - `genre` : `,`로 구분되었으며 스팀에서 지정한 게임 장르로 추측됨
- `tag`
  - `appid`, `name`
  - `tag` : 유저가 투표할 수 있는 게임의 특성. JSON으로 저장했음
- `lang_genre`, `tag`은 SQL에는 한 특성 속에 넣었으며 분석 시 가공하여 이용할 예정
  - `lang`은 영어 & 한국어 & 중국어(간체, 번체) & 일본어만 이용할 예정
  - `genre`은 너무 적은 수의 데이터는 제외함
  - `tag`은 가공한 적이 있으나, 그 양이 너무 많아서 렉이 걸리는 이슈로 일단 저런 형태로 저장만 해뒀음.

#### 자동화
- 도커를 사용했으며, `docker compose`를 이용해 2개의 컨테이너를 띄웠음
  - `steamspy-collector` : 파이썬 스크립트가 있으며 `steamspy`에서 데이터를 수집해 `steamspy-mysql` 컨테이너에 저장함. 스크립트는 `schedule` 라이브러리를 이용, 6시간에 1번씩 실행됨
  - `steamspy-mysql` : `mysql` 컨테이너로, 수집된 데이터를 저장하는 역할. 호스트 OS에선 `localhost:33060` 으로, `steamspy-collector`에선 `steamspy-mysql:3306`으로 접근할 수 있다.

### 분석


## 수정 내역

### `230614`
- 사이트 정상 작동 & 5000개 데이터 수집 이후 디테일 수집하는 과정에서 `raw_detail`에 데이터가 정상적으로 수집되지 않는 문제 해결

