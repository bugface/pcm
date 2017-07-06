import jellyfish

def calc_sim_l(str_pair):
	return jellyfish.levenshtein_distance(str_pair[0], str_pair[1])

def calc_sim_dl(str_pair):
	return jellyfish.damerau_levenshtein_distance(str_pair[0], str_pair[1])



def main():
	pass

if __name__ == '__main__':
	main()
