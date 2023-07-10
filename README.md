# steamspypi 데이터 수집 및 분석 프로젝트

- [개발 일지](https://github.com/dowrave/TIL/blob/main/Obsidian/%ED%94%84%EB%A1%9C%EC%A0%9D%ED%8A%B8%20%EC%83%81%EC%9E%90/%EC%8A%A4%ED%8C%80%20%EB%8D%B0%EC%9D%B4%ED%84%B0%20%EB%B6%84%EC%84%9D/%EC%9D%BC%EC%A7%80.md)

## 프로젝트 과정
- `steamspypi`를 이용, 보유자 수 기준 상위 5000개의 게임에 대한 데이터를 수집 및 분석
- steamspy 데이터 수집(`230515~`)
- 자동화(`230531 ~ `)
- 스팀 리뷰 데이터 수집(`230707~`)

### 사용 라이브러리
- `requirements.txt`에 대부분 있음
- 유의 : `KoNLPY`는 Java를 필요로 하지만 이 가상환경에 Java 설치를 하진 않은 상태임
  - `KoreanReview.py`를 실행하려면 Java 설치 & 환경 변수 `JAVA_HOME` 지정이 필요함(이건 할지 말지 모르겠음)

## 프로젝트 내용

### steamspy에서 데이터 수집
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

