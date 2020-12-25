#include <cstdio>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <fstream>

//const static int key = 10086;
const static char* f = "./key";

namespace sig_sync{

class IPCLock {
public:
    int shmid;
    int fd;
    char counter[1];
    int n_process;
    IPCLock(int n_process_): n_process(n_process_) {
    }
    ~IPCLock() {
        
    }
    void incre() {
        // There requires a lock to void preempting, but I'm too lazy
        std::ifstream in(f);
        in >> counter;
        in.close();
        printf("[++++PRE++++]%d\n", (int)*counter);
        counter[0] = counter[0] + 1;
        printf("[++++NOW++++]%d\n", (int)*counter);
        std::ofstream out(f);
        out << counter;
        out.close();
    }

    void clear() {
        ofstream out(f);
        out << '0';
        out.close();
    }

    void wait() {
        while(true) {
            fd = open(f, O_RDONLY, S_IRUSR | S_IWUSR);
            read(fd, counter, 1);
            if (int(*counter) == n_process) {
                printf("break\n");
                break;
            }
            close(fd);
            // fd = open(f, O_RDONLY, S_IRUSR | S_IWUSR);
            // std::ifstream in(f);
            // in >> counter;
            
            // if (int(*counter) == '0' + n_process) {
            //     printf("break\n");
            //     break;
            // }
            // in.close();
        }
    }
};

}