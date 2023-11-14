import random as rng
import string


def _genString(N):
	return "".join(rng.choices(string.ascii_lowercase, k=N))

def _lpad(value, width, fillchar='0'):
	return str(value).rjust(width, fillchar)

def _genColName(range, startChar='col'):
	return startChar+str(rng.randrange(1, range))

def gen_dummy_data(testType, maxKey, N = 1):
	
    if "INSERT" == testType :
        dummy_data = [
			_lpad(rng.randrange(1, maxKey), len(str(maxKey)) + 1),
			_genString(10),
			_genString(10),
			_genString(10),
			_genString(10),
            _genString(10),
			_genString(10),
			_genString(10),
			_genString(10)
		]
    elif "MULTI_INSERT" == testType :
        dummy_data = []
        for i in range(N) :
            bufferData = [
				_lpad(rng.randrange(1, maxKey), len(str(maxKey)) + 1),
				_genString(10),
				_genString(10),
				_genString(10),
				_genString(10),
				_genString(10),
				_genString(10),
				_genString(10),
				_genString(10)
			]
            bufferString = "','".join(bufferData)
            dummy_data.append(f"'{bufferString}'")
    elif "UPDATE" == testType :
        dummy_data = {
			"colKey": _lpad(rng.randrange(1, maxKey), len(str(maxKey)) + 1),
			"col1": _genString(10),
            "col2": _genString(10),
            "col3": _genString(10),
            "col4": _genString(10),
            "col5": _genString(10),
		}
    else :
        dummy_data = {
			"colKey": _lpad(rng.randrange(1, maxKey), len(str(maxKey)) + 1),
		}
    return dummy_data
