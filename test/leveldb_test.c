#include "c.h"
#include <stdio.h>

int main() {
  leveldb_t *db;
  leveldb_options_t *options;
  leveldb_readoptions_t *roptions;
  leveldb_writeoptions_t *woptions;
  char *err = NULL;
  char *read;
  size_t read_len;

  /******************************************/
  /* OPEN */

  options = leveldb_options_create();
  leveldb_options_set_create_if_missing(options, 1);
  db = leveldb_open(options, "testdb", &err);

  if (err != NULL) {
    fprintf(stderr, "Open fail.\n");
    return(1);
  }

  /* reset error var */
  leveldb_free(err); err = NULL;

  /******************************************/
  /* WRITE */

  woptions = leveldb_writeoptions_create();
  leveldb_put(db, woptions, "key", 3, "value", 5, &err);

  if (err != NULL) {
    fprintf(stderr, "Write fail.\n");
    return(1);
  }

  leveldb_free(err); err = NULL;

  /******************************************/
  /* READ */

  roptions = leveldb_readoptions_create();
  read = leveldb_get(db, roptions, "key", 3, &read_len, &err);

  if (err != NULL) {
    fprintf(stderr, "Read fail.\n");
    return(1);
  }

  printf("key: %s\n", read);

  leveldb_free(err); err = NULL;

  /******************************************/
  /* WRITE */

  woptions = leveldb_writeoptions_create();
  leveldb_put(db, woptions, "key1", 4, "value1", 6, &err);

  if (err != NULL) {
    fprintf(stderr, "Write fail.\n");
    return(1);
  }

  leveldb_free(err); err = NULL;

  /******************************************/
  /* READ */

  roptions = leveldb_readoptions_create();
  read = leveldb_get(db, roptions, "key1", 4, &read_len, &err);

  if (err != NULL) {
    fprintf(stderr, "Read fail.\n");
    return(1);
  }

  printf("key1: %s\n", read);

  leveldb_free(err); err = NULL;

  /******************************************/
  /* DELETE */

  leveldb_delete(db, woptions, "key", 3, &err);

  if (err != NULL) {
    fprintf(stderr, "Delete fail.\n");
    return(1);
  }

  leveldb_free(err); err = NULL;

  /******************************************/
  /* RE-READ */
  read = leveldb_get(db, roptions, "key", 3, &read_len, &err);

  if (err != NULL) {
    fprintf(stderr, "Read fail.\n");
    return(1);
  }

  printf("key: %s\n", read);

  leveldb_free(err); err = NULL;

  /******************************************/
  /* CLOSE */

  leveldb_close(db);

  /******************************************/
  /* Iterators */
  size_t len;
  const char* str;
  options = leveldb_options_create();
  leveldb_options_set_create_if_missing(options, 1);
  db = leveldb_open(options, "testdb", &err);
  leveldb_put(db, woptions, "key2", 4, "value2", 6, &err);
  leveldb_put(db, woptions, "key3", 4, "value3", 6, &err);
  leveldb_put(db, woptions, "key4", 4, "value4", 6, &err);
  
  leveldb_iterator_t* iter = leveldb_create_iterator(db, roptions);
  leveldb_iter_seek_to_first(iter);
  str = leveldb_iter_key(iter, &len);
  printf("First: %s \n", str);

  leveldb_iter_seek_to_last(iter);
  str = leveldb_iter_key(iter, &len);
  printf("Last: %s \n", str);
  
  leveldb_close(db);
  /******************************************/
  /* DESTROY */

  leveldb_destroy_db(options, "testdb", &err);

  if (err != NULL) {
    fprintf(stderr, "Destroy fail.\n");
    return(1);
  }

  leveldb_free(err); err = NULL;


  return(0);
}
