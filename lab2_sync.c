/*
*   DKU Operating System Lab
*           Lab2 (Hash Queue Lock Problem)
*           Student id : 32204913
*           Student name : 허준서
*
*   lab2_sync.c :
*       - lab2 main file.
*		- Thread-safe Hash Queue Lock code
*		- Coarse-grained, fine-grained lock code
*       - Must contains Hash Queue Problem function's declations.
*
*   Implement thread-safe Hash Queue for coarse-grained verison and fine-grained version.
*/

#include <aio.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>
#include <pthread.h>
#include <asm/unistd.h>

#include "lab2_sync_types.h"

void init_mutexs() {
	pthread_mutex_init(&cg_mutex, NULL);
	pthread_mutex_init(&fg_mutex_queue, NULL);
	for (int i = 0; i < HASH_SIZE; i++) {
		pthread_mutex_init(&fg_mutex_hash[i], NULL);
	}
}

void print_queue() {
	queue_node* current_node = front;
	printf("Queue: ");
	while (current_node->next) {
		current_node = current_node->next;
		printf("%d ", current_node->data);
	}
	printf("\n");
}

void print_hashtable() {
	for (int i = 0; i < HASH_SIZE; i++) {
		printf("hashlist[%d]: ", i);
		queue_node* current_node = hashlist[i]->q_loc;
		while (current_node->hnext) {
			current_node = current_node->hnext;
			printf("%d ", current_node->data);
		}
		printf("\n");
	}
}

/*
 * TODO
 *  Implement function which init queue nodes for front and rear
 *  ( refer to the ./include/lab2_sync_types.h for front and rear nodes)
 */
void init_queue() {
	// You need to implement init_queue function.
	// front is always an empty queue_node.
	front = rear = (queue_node*)malloc(sizeof(queue_node));
}

/*
 * TODO
 *  Implement function which add new_node at next rear node
 *
 *  @param queue_node *new_node		: Node which you need to insert at queue.
 */
void enqueue(queue_node* new_node) {
	// You need to implement enqueue function.
	rear->next = new_node;
	new_node->prev = rear;
	rear = new_node;
}

/*
 * TODO
 *  Implement function which add new_node at next rear node
 *
 *  @param queue_node *new_node		: Node which you need to insert at queue in coarse-grained manner.
 */
void enqueue_cg(queue_node* new_node) {
	// You need to implement enqueue_cg function.
	// pthread_mutex_lock(&cg_mutex);
	// rear->next = new_node;
	// new_node->prev = rear;
	// rear = new_node;
	// pthread_mutex_unlock(&cg_mutex);
	pthread_mutex_lock(&cg_mutex);
	enqueue(new_node);
	pthread_mutex_unlock(&cg_mutex);
}

/*
 * TODO
 *  Implement function which add new_node at next rear node
 *
 *  @param queue_node *new_node		: Node which you need to insert at queue in fine-grained manner.
 */
void enqueue_fg(queue_node* new_node) {
	// You need to implement enqueue_fg function.
	pthread_mutex_lock(&fg_mutex_queue);
	rear->next = new_node;
	new_node->prev = rear;
	rear = new_node;
	pthread_mutex_unlock(&fg_mutex_queue);
}

/*
 * TODO
 *  Implement function which delete del_node at location that contains target key
 *
 *  @param queue_node *del_node		: Node which you need to delete at queue.
 */
void dequeue(queue_node* del_node) {
	// You need to implement dequeue function.
	queue_node* current_node = front;
	while (current_node->next) {
		current_node = current_node->next;
		if (current_node->data == del_node->data) {
			if (current_node == rear) {
				current_node->prev->next = NULL;
				return;
			}
			current_node->prev->next = current_node->next;
			current_node->next->prev = current_node->prev;
			return;
		}
	}
}

/*
 * TODO
 *  Implement function which delete del_node at location that contains target key
 *
 *  @param queue_node *del_node		: Node which you need to delete at queue in coarse-grained manner.
 */
void dequeue_cg(queue_node* del_node) {
	// You need to implement dequeue_cg function.
	pthread_mutex_lock(&cg_mutex);
	dequeue(del_node);
	pthread_mutex_unlock(&cg_mutex);
}

/*
 * TODO
 *  Implement function which delete del_node at location that contains target key
 *
 *  @param queue_node *del_node		: Node which you need to delete at queue in fine-grained manner.
 */
void dequeue_fg(queue_node* del_node) {
	// You need to implement dequeue_fg function.
	pthread_mutex_lock(&fg_mutex_queue);
	queue_node* current_node = front;
	while (current_node->next) {
		current_node = current_node->next;
		if (current_node->data == del_node->data) {
			if (current_node == rear) {
				current_node->prev->next = NULL;
				break;
			}
			current_node->prev->next = current_node->next;
			current_node->next->prev = current_node->prev;
			break;
		}
	}
	pthread_mutex_unlock(&fg_mutex_queue);
}

/*
 * TODO
 *  Implement function which init hashlist(same as hashtable) node.
 */
void init_hlist_node() {
	// You need to implement init_hlist_node function.
	for (int i = 0; i < HASH_SIZE; i++) {
		queue_node* node = (queue_node*)malloc(sizeof(queue_node));
		hashlist[i] = (hlist_node*)malloc(sizeof(hlist_node));
		hashlist[i]->q_loc = node;
	}
}

/*
 * TODO
 *  Implement function which calculate hash value with modulo operation.
 */
int hash(int val) {
	// You need to implement hash function.
	return val % HASH_SIZE;
}

/*
 * TODO
 *  Implement function which insert the resilt of finding the location
 *  of the bucket using value to the entry and hashtable
 *
 *  @param hlist_node *hashtable		: A pointer variable containing the bucket
 *  @param int val						: Data to be stored in the queue node
 */
void hash_queue_add(hlist_node* hashtable, int val) {
	// You need to implement hash_queue_add function.
	queue_node* new_node = (queue_node*)malloc(sizeof(queue_node));
	queue_node* current_node;
	new_node->data = val;

	current_node = hashtable->q_loc;
	while (current_node->hnext) {
		current_node = current_node->hnext;
	}
	// Now current_node is tail node. Set hash link
	current_node->hnext = new_node;
	new_node->hprev = current_node;
	// Set queue
	enqueue(new_node);
	// print_queue();
}

/*
 * TODO
 *  Implement function which insert the resilt of finding the location
 *  of the bucket using value to the entry and hashtable
 *
 *  @param hlist_node *hashtable		: A pointer variable containing the bucket
 *  @param int val						: Data to be stored in the queue node
 */
void hash_queue_add_cg(hlist_node* hashtable, int val) {
	// You need to implement hash_queue_add_cg function.
	queue_node* new_node = (queue_node*)malloc(sizeof(queue_node));
	queue_node* current_node;
	new_node->data = val;

	pthread_mutex_lock(&cg_mutex);
	current_node = hashtable->q_loc;
	while (current_node->hnext) {
		current_node = current_node->hnext;
	}
	// Now current_node is tail node. Set hash link
	current_node->hnext = new_node;
	new_node->hprev = current_node;
	// Set queue
	pthread_mutex_unlock(&cg_mutex);
	enqueue_cg(new_node);
	// print_queue();
}

/*
 * TODO
 *  Implement function which insert the resilt of finding the location
 *  of the bucket using value to the entry and hashtable
 *
 *  @param hlist_node *hashtable		: A pointer variable containing the bucket
 *  @param int val						: Data to be stored in the queue node
 */
void hash_queue_add_fg(hlist_node* hashtable, int val) {
	// You need to implement hash_queue_add_fg function.
	queue_node* new_node = (queue_node*)malloc(sizeof(queue_node));
	queue_node* current_node;
	pthread_mutex_lock(&fg_mutex_hash[hash(val)]);
	new_node->data = val;
	current_node = hashtable->q_loc;
	while (current_node->hnext) {
		current_node = current_node->hnext;
	}
	// Now current_node is tail node. Set hash link
	current_node->hnext = new_node;
	new_node->hprev = current_node;
	pthread_mutex_unlock(&fg_mutex_hash[hash(val)]);
	// Set queue
	enqueue_fg(new_node);
	// print_queue();
}

/*
 * TODO
 *  Implement function which check if the data(value) to be stored is in the hashtable
 *
 *  @param int val						: variable needed to check if data exists
 *  @return								: status (success or fail)
 */
int value_exist(int val) {
	// You need to implement value_exist function.
	hlist_node* hashtable = hashlist[hash(val)];
	queue_node* current_node = hashtable->q_loc;
	while (current_node->next) {
		current_node = current_node->next;
		if (current_node->data == val) return 1;
	}
	return 0;
}

/*
 * TODO
 *  Implement function which find the bucket location using target
 */
void hash_queue_insert_by_target() {
	// You need to implement hash_queue_insert_by_target function.
	hash_queue_add(hashlist[hash(target)], target);
	// printf("[%d] ", target);
}

/*
 * TODO
 *  Implement function which find the bucket location using target
 */
void hash_queue_insert_by_target_cg() {
	// You need to implement hash_queue_insert_by_target_cg function.
	hash_queue_add_cg(hashlist[hash(target)], target);
}

/*
 * TODO
 *  Implement function which find the bucket location using target
 */
void hash_queue_insert_by_target_fg() {
	// You need to implement hash_queue_insert_by_target_fg function.
	hash_queue_add_fg(hashlist[hash(target)], target);
}

/*
 * TODO
 *  Implement function which find the bucket location and stored data
 *  using target and delete node that contains target
 */
void hash_queue_delete_by_target() {
	// You need to implement hash_queue_delete_by_target function.
	int flag = 0;
	hlist_node* hashtable = hashlist[hash(target)];
	queue_node* current_node = hashtable->q_loc;

	while (current_node->hnext) {
		current_node = current_node->hnext;
		if (current_node->data == target) {
			flag = 1;
			if (current_node->hnext == NULL) { // if tail
				current_node->hprev->hnext = NULL;
				break;
			}
			current_node->hprev->hnext = current_node->hnext;
			current_node->hnext->hprev = current_node->hprev;
			break;
		}
	}
	if (flag) dequeue(current_node);
}

/*
 * TODO
 *  Implement function which find the bucket location and stored data
 *  using target and delete node that contains target
 */
void hash_queue_delete_by_target_cg() {
	// You need to implement hash_queue_delete_by_target_cg function.
	int flag = 0;
	pthread_mutex_lock(&cg_mutex);
	hlist_node* hashtable = hashlist[hash(target)];
	queue_node* current_node = hashtable->q_loc;

	while (current_node->hnext) {
		current_node = current_node->hnext;
		if (current_node->data == target) {
			flag = 1;
			if (current_node->hnext == NULL) { // if tail
				current_node->hprev->hnext = NULL;
				break;
			}
			current_node->hprev->hnext = current_node->hnext;
			current_node->hnext->hprev = current_node->hprev;
			break;
		}
	}
	pthread_mutex_unlock(&cg_mutex);
	if (flag) dequeue_cg(current_node);
}

/*
 * TODO
 *  Implement function which find the bucket location and stored data
 *  using target and delete node that contains target
 */
void hash_queue_delete_by_target_fg() {
	// You need to implement hash_queue_delete_by_target_fg function.
	int flag = 0;
	int val = target;

	pthread_mutex_lock(&fg_mutex_hash[hash(val)]);
	hlist_node* hashtable = hashlist[hash(val)];
	queue_node* current_node = hashtable->q_loc;

	while (current_node->hnext) {
		current_node = current_node->hnext;
		if (current_node->data == target) {
			flag = 1;
			if (current_node->hnext == NULL) { // if tail
				current_node->hprev->hnext = NULL;
				break;
			}
			current_node->hprev->hnext = current_node->hnext;
			current_node->hnext->hprev = current_node->hprev;
			break;
		}
	}
	pthread_mutex_unlock(&fg_mutex_hash[hash(val)]);
	if (flag) dequeue_fg(current_node);
}

