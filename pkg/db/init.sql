use test;
drop table if exists `users`;
create table `users` (
    `id` int unsigned not null,
    `name` varchar(15) not null,
    `address` varchar(20) not null,
    `continent` varchar(13) not null,
    PRIMARY KEY (`id`),
    INDEX (`name`),
    INDEX (`address`)
);