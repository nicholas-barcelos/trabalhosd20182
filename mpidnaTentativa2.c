#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <limits.h>

// MAX char table (ASCII)
#define MAX 256

// Processo mestre
#define MASTER 0

// Arquivos: dna.in, query.in, dna.out
FILE *fdatabase, *fquery, *fout;

// Strings: bases lidas do dna.in e queries do query.in
char *bases, *str;

// Rank do processo e qtd de processos
int my_rank, processos;

// Inicio, fim e tamanho da partição
int start, end, tamDaParticao;

// Tags
#define PART_START 100
#define PART_END 200
#define PART_SIZE 300

// Boyers-Moore-Hospool-Sunday algorithm for string matching
int bmhs(char *string, int n, char *substr, int m) {

	int d[MAX];
	int i, j, k;

	// pre-processing
	for (j = 0; j < MAX; j++)
		d[j] = m + 1;
	for (j = 0; j < m; j++)
		d[(int) substr[j]] = m - j;

	// searching
	i = m - 1;
	while (i < n) {
		k = i;
		j = m - 1;
		while ((j >= 0) && (string[k] == substr[j])) {
			j--;
			k--;
		}
		if (j < 0)
			return k + 1;
		i = i + d[(int) string[i + 1]];
	}

	return INT_MAX;
}

void openfiles() {

	fdatabase = fopen("dna.in", "r+");
	if (fdatabase == NULL) {
		perror("dna.in");
		exit(EXIT_FAILURE);
	}

	fquery = fopen("query.in", "r");
	if (fquery == NULL) {
		perror("query.in");
		exit(EXIT_FAILURE);
	}

	fout = fopen("dna.out", "w");
	if (fout == NULL) {
		perror("fout");
		exit(EXIT_FAILURE);
	}

}

void closefiles() {
	fflush(fdatabase);
	fclose(fdatabase);

	fflush(fquery);
	fclose(fquery);

	fflush(fout);
	fclose(fout);
}

void remove_eol(char *line) {
	int i = strlen(line) - 1;
	while (line[i] == '\n' || line[i] == '\r') {
		line[i] = 0;
		i--;
	}
}

int main(int argc, char** argv) {

	MPI_Init(&argc, &argv);
	MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
	MPI_Comm_size(MPI_COMM_WORLD, &processos);
	MPI_Status status;
	clock_t begin = clock();

	int tag = 0;

	bases = (char*) malloc(sizeof(char) * 1000001);
	if (bases == NULL) {
		perror("malloc bases");
		exit(EXIT_FAILURE);
	}
	str = (char*) malloc(sizeof(char) * 1000001); //1milhão de chars
	if (str == NULL) {
		perror("malloc str");
		exit(EXIT_FAILURE);
	}

	openfiles();

	char desc_dna[100], desc_query[100];
	char line[100];
	int i, found, result;

	fgets(desc_query, 100, fquery); //pega o nome da query
	remove_eol(desc_query);

	while (!feof(fquery)) {
		if(my_rank==MASTER){
			fprintf(fout, "%s\n", desc_query); //escreve o nome da query na saida
		}
		// read query string
		fgets(line, 100, fquery); //pega a query
		remove_eol(line);
		str[0] = 0;
		i = 0;
		do {
			strcat(str + i, line);
			if (fgets(line, 100, fquery) == NULL)
				break;
			remove_eol(line);
			i += 80;
		} while (line[0] != '>');
		strcpy(desc_query, line);
		
        // read database and search
		found = 0;
		fseek(fdatabase, 0, SEEK_SET);
		fgets(line, 100, fdatabase); //pega de 100 em 100 de fdatabase
		remove_eol(line);

		while (!feof(fdatabase)) {
			strcpy(desc_dna, line); //pega nome da sequencia
			bases[0] = 0;
			i = 0;
			fgets(line, 100, fdatabase);
			remove_eol(line);
			
			do {
				strcat(bases + i, line); //concatena bases com a sequencia lida de databases, colocando 
				if (fgets(line, 100, fdatabase) == NULL)
					break;
				remove_eol(line);
				i += 80;
			} while (line[0] != '>');


            //parte para paralelizar
            start = (strlen(bases) / processos) * my_rank;
            end = ((strlen(bases) / processos) * (my_rank + 1))-1;

			if(my_rank + 1 == processos){
				if(end != strlen(bases) - 1){
					end = strlen(bases) - 1;
				}
			}
			else{
            	end += (strlen(str) - 1);
			}
			
			tamDaParticao = end - start + 1;


			char* substring =  (char*) malloc (sizeof(char) * (tamDaParticao+1));
			strncpy(substring, bases+start, tamDaParticao);

			
			result = bmhs(substring, tamDaParticao, str, strlen(str)); //retorna a posição onde foi encontrada a substring
			

			if(my_rank == MASTER){
				//fprintf(fout, "%s\n", desc_query); //escreve o nome da query na saida
				int outroResult;
				for(int i = 1; i < processos; i++){
					MPI_Recv(&outroResult, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					if(result>outroResult)
						result=outroResult;
				}
				//printf("result = %d\n",result);

				if (result < INT_MAX) {
					//printf("\nresult: %d, rank:%d\nsubstring:%s\nstr:%s\n\n", result+start, my_rank, substring, str);
					fprintf(fout, "%s\n%d\n", desc_dna, result);//escreve o nome da sequencia onde foi encontrada e a posição
					found++;
				}
			}
			else{
				if(result < INT_MAX)
					result+=start;
				MPI_Send(&result, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
			}



			// DESALOCAR A SUBSTRING DA BASE
			free(substring);
		}

		if (!found&&my_rank==MASTER){
			fprintf(fout, "NOT FOUND\n");
		}
	}

	closefiles();

	free(str);
	free(bases);
	
	if(my_rank==MASTER){
		clock_t end = clock();
		double time = (double) (end - begin)/CLOCKS_PER_SEC;
		printf("O processamento da matriz de %dx%d levou %f segundos\n", size, size, time);
	}
	
	MPI_Finalize();

	return EXIT_SUCCESS;
}
