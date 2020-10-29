
#ifndef _LATCH_H_
#define _LATCH_H_

#include <iostream>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <random>
#include <chrono>

// static std::default_random_engine e(time(0));
// static std::uniform_int_distribution<unsigned> u(0, 9); //随机数分布对象

class Latch {
	public:
		Latch(int limit) {
			this->limit_ = limit;
            this->lt_ = limit;
		}

		virtual void await() = 0;
		virtual void count_down() = 0;
		virtual int get_unarrived() = 0;
        virtual void reset() = 0;
	protected:
		int limit_;
        int lt_;
};

class CountDownLatch : public Latch {
	public:
		using Latch::Latch;

	void await() override {
		std::unique_lock<std::mutex> lk(mtx_);
		cv_.wait(lk, [&]{
			//std::cout << "limit_: " << limit_ << std::endl;
			return (limit_ == 0);
			});
	}

	void count_down() override {
		std::unique_lock<std::mutex> lk(mtx_);
		limit_--;
		cv_.notify_all();
	}

	int get_unarrived() override {
		std::unique_lock<std::mutex> lk(mtx_);
		return limit_;
	}

    void reset() override {
		std::unique_lock<std::mutex> lk(mtx_);
        limit_ = lt_;
	}
	private:
		std::mutex mtx_;
		std::condition_variable cv_;
};

#endif
// class ProgrammerTravel {
//     public:
// 	ProgrammerTravel(Latch* latch, std::string name, std::string vehicle){
// 		latch_ = latch;
// 		vehicle_ = vehicle;
// 		name_ = name;
// 	}
// 	~ProgrammerTravel(){
// 		thd_.join();
// 	}

// 	void run() {
// 		thd_ = std::thread([&]{
// 			int sec = u(e);
// 			std::cout<< name_ << " take " << vehicle_ << ", need time: " << sec << "."<< std::endl;
// 			std::this_thread::sleep_for(std::chrono::seconds(sec));
// 			std::cout << name_ << " arrived." << std::endl;
// 			latch_->count_down();
// 		});

// 	}

// 	private:
// 	Latch* latch_;
// 	std::string vehicle_;
// 	std::string name_;
// 	std::thread thd_;
// };

// int main(int argc, char** argv) {
// 	Latch* latch = new CountDownLatch(4);
// 	ProgrammerTravel li(latch, "Li", "Bus");
// 	ProgrammerTravel liu(latch, "Liu", "bike");
// 	ProgrammerTravel lv(latch, "Lv", "subway");
// 	ProgrammerTravel zhang(latch, "Zhang", "walk");

// 	li.run();
// 	liu.run();
// 	lv.run();
// 	zhang.run();

// 	latch->await();
// 	std::cout << "all persons arrived finally." << std::endl;
// 	delete latch;
// }

