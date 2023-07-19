from konlpy.tag import Kkma

try:
    kkma = Kkma()
    print("KoNLPy가 정상적으로 설치되었습니다.")
except ImportError:
    print("KoNLPy 설치에 실패했습니다.")