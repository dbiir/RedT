#ifndef MEMCACHED_UTIL_H
#define MEMCACHED_UTIL_H

#include <libmemcached/memcached.h>
#include <stdio.h>
#include <assert.h>

static char REG_IP[] = "10.77.110.148";

__thread memcached_st *memc = NULL;

memcached_st* m_memc_create()
{
    memcached_server_st *servers = NULL;
    memcached_st *memc = memcached_create(NULL);
    memcached_return rc;

    memc = memcached_create(NULL);
    char *registry_ip = REG_IP;

    /* We run the memcached server on the default memcached port */
    servers = memcached_server_list_append(servers,
        registry_ip, MEMCACHED_DEFAULT_PORT, &rc);
    rc = memcached_server_push(memc, servers);
    if (rc != MEMCACHED_SUCCESS) {
        printf("Couldn't add memcached server.\n");
    } 

    return memc;
}

void m_memc_publish(const char *key, void *value, int len)
{
    assert(key != NULL && value != NULL && len > 0);
    memcached_return rc;

    if(memc == NULL) {
        memc = m_memc_create();
    }

    rc = memcached_set(memc, key, strlen(key), (const char *) value, len, 
        (time_t) 0, (uint32_t) 0);
    if (rc != MEMCACHED_SUCCESS) {
        char *registry_ip = REG_IP;
        fprintf(stderr, "\tHRD: Failed to publish key %s. Error %s. "
            "Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
        exit(-1);
    }
}

int m_memc_get_published(const char *key, void **value)
{
    assert(key != NULL);
    if(memc == NULL) {
        memc = m_memc_create();
    }

    memcached_return rc;
    size_t value_length;
    uint32_t flags;

    *value = memcached_get(memc, key, strlen(key), &value_length, &flags, &rc);

    if(rc == MEMCACHED_SUCCESS ) {
        return (int) value_length;
    } else if (rc == MEMCACHED_NOTFOUND) {
        assert(*value == NULL);
        return -1;
    } else {
        char *registry_ip = REG_IP;
        fprintf(stderr, "HRD: Error finding value for key \"%s\": %s. "
            "Reg IP = %s\n", key, memcached_strerror(memc, rc), registry_ip);
        exit(-1);
    }
    
    /* Never reached */
    assert(false);
}

#endif // MEMCACHED_UTIL_H
