package com.zubayer.bookingingestservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class SchedulingConfig {

    /**
     * The outbox relay runs on a dedicated PLATFORM thread.
     * <p>
     * With spring.threads.virtual.enabled=true, Spring Boot schedules @Scheduled tasks
     * on virtual threads. Under a flash sale the relay then competes for carrier threads
     * with every in-flight request — and the measured result was that it stopped running
     * entirely for the duration of the burst.
     * <p>
     * A platform thread is scheduled by the OS, independently of the virtual thread
     * carrier pool, so draining the outbox keeps pace while requests are saturating
     * the service.
     */

    @Bean
    public TaskScheduler taskScheduler() {

        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();

        scheduler.setPoolSize(2);
        scheduler.setThreadNamePrefix("outbox-relay-");
        scheduler.setAwaitTerminationSeconds(30);
        scheduler.setWaitForTasksToCompleteOnShutdown(true);

        return scheduler;

    }

}
