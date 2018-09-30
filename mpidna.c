#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

// MAX char table (ASCII)
#define MAX 256

// Processo mestre
#define MASTER 0

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

	return -1;
}

FILE *fdatabase, *fquery, *fout;

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

	fout = fopen("mpidna.out", "w");
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

char *bases;
char *query;
char **processedQueries;

int queryIndex = 0;

int main(int argc, char **argv) {
	int my_rank, np; // rank e número de processos
	int tag = 200;
	MPI_Status status;

	MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &my_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &np);

	// Malloc das queries dos processos
	
	processedQueries = malloc(sizeof(char*) * np);
	if (processedQueries == NULL) {
		perror("malloc processedQueries");
		exit(EXIT_FAILURE);
	}else{
		for(int i = 0; i < np; i++){
			processedQueries[i] = (char*) malloc(sizeof(char) * 1000001);
			if (processedQueries[i] == NULL) {
				perror("malloc processedQueries i");
				exit(EXIT_FAILURE);
			}
		}
	}
	
	char desc_dna[100], desc_query[100];
	char line[100];
	int i, found, result;
	
	if(my_rank == MASTER)
	{
		bases = (char*) malloc(sizeof(char) * 1000001);
		if (bases == NULL) {
			perror("malloc bases");
			exit(EXIT_FAILURE);
		}
		query = (char*) malloc(sizeof(char) * 1000001);
		if (query == NULL) {
			perror("malloc query");
			exit(EXIT_FAILURE);
		}
		
		openfiles();

		int index = 0;
		while (!feof(fquery)) 
		{	
			// Pega a descrição da query
			fgets(desc_query, 100, fquery);
			remove_eol(desc_query);
			
			// Leitura
			fgets(line, 100, fquery);
			remove_eol(line);
			// Vetor pra armazenamento 
			query[0] = 0;
			i = 0;
			do 
			{
				strcat(query + i, line);
				if (fgets(line, 100, fquery) == NULL)
					break;
				remove_eol(line);
				i += 80;
			} while (line[0] != '>');
			
			// Armazena a informaçâo
			if(index = 0)
			{
				strcpy(processedQueries[queryIndex],query);
				queryIndex++;
			}
			// Envio
			else
			{
				//Fazer envio da descrição
				MPI_Send(&query, 1, MPI_CHAR, index, tag, MPI_COMM_WORLD);
			}
			if(index == np - 1)
			{
				index = 0;
			}
			else
			{
				index++;
			}
			
		}
		for(int i =1; i < np; i++){
			MPI_Send("FIM SEND", 1, MPI_CHAR, i, tag, MPI_COMM_WORLD);
		}
	}
	else
	{
		//Recebe e armazena a informação
		MPI_Recv(&val, 1, MPI_CHAR, 0, tag, MPI_COMM_WORLD, &status);
		strcpy(processedQueries[queryIndex],query);
		queryIndex++;
		
	}
	
	//Apos a distribuição das queries começar a brincadeira
	
	if(my_rank == MASTER)
	{
		while (!feof(fdatabase)) 
		{
			// Faz a leitura e envia
			fgets(desc_dna, 100, fdatabase);
			remove_eol(desc_dna);
			fgets(line, 100, fdatabase);
			remove_eol(line);
			bases[0] = 0;
			i = 0;
			do 
			{
				strcat(bases + i, line);
				if (fgets(line, 100, fdatabase) == NULL)
					break;
				remove_eol(line);
				i += 80;
			} while (line[0] != '>');
			// Envia para todos os processos a informaçâo
			for(int i =1; i < np;i++){
				
				MPI_Send(&line, 1, MPI_CHAR, i, tag, MPI_COMM_WORLD);
			}
			for(int i = 0;i < queryIndex;i++){
				// Erro ?
				query = processedQueries[i];
				result = bmhs(line, strlen(line), query, strlen(query));
				if (result > 0) {
					//falta a desc
					fprintf(fout, "%s\n%d\n", desc_dna, result);
					//???
					found++;
				}				
			}
			
			
		}
	}
	else
	{
		// Recebe e processa a informaçâo
		MPI_Recv(&line, 1, MPI_FLOAT, source, tag, MPI_COMM_WORLD, &status);
		for(int i = 0; i < queryIndex; i++){
			//o q tá errado?
			query = processedQueries[i];
			result = bmhs(line, strlen(line), query, strlen(query));
			if (result > 0) {
				//falta a desc
				fprintf(fout, "%s\n%d\n", desc_dna, result);
				found++;
			}				
		}
	}
	
	if(my_rank == MASTER){
		closefiles();
	}
	

	free(query);
	free(bases);
	free(processedQueries);
	
	MPI_Finalize();
	
	return EXIT_SUCCESS;
}

