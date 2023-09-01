const util = require('util');
const sqlite3 = require('sqlite3');
const uuidGenerator = require('uuid').v4;
const redis = require('redis');
const RedLock = require('redlock');
const mysql = require('mysql2/promise');

const getCurrentTimestamp = require('./helpers/getCurrentTimestamp');
const QueueClient = require('./QueueClient');
const Sqlite3Driver = require('./drivers/Sqlite3Driver');
const MysqlDriver = require('./drivers/MysqlDriver');
const RedisDriver = require('./drivers/RedisDriver');
const Worker = require('./Worker');

/**
 * VerySimpleQueue is a client for managing a simple job queue.
 * @class
 */
class VerySimpleQueue {
  /** @type {string[]} */
  #supportedDrivers;

  /** @type {QueueClient} */
  #queueClient;

  /**
   * Creates an instance of VerySimpleQueue.
   * @param {string} driverName - The name of the driver ('sqlite3', 'redis', or 'mysql').
   * @param {module:types.Sqlite3DriverConfig | Object} driverConfig - Driver-specific configuration.
   * @throws {Error} If the specified driver is not supported.
   *
   * @example <caption>Create VerySimpleQueue with SQLite3 driver</caption>
   * new VerySimpleQueue('sqlite3', { filePath: '/tmp/db.sqlite3' });
   *
   * @example <caption>Create VerySimpleQueue with Redis driver</caption>
   * new VerySimpleQueue('redis', {}); // Options: https://github.com/NodeRedis/node-redis#options-object-properties
   *
   * @example <caption>Create VerySimpleQueue with MySQL driver</caption>
   * new VerySimpleQueue('mysql', {
   *    host: 'localhost',
   *    user: 'root',
   *    password: 'root',
   *    database: 'queue',
   *  }); // Options: https://github.com/mysqljs/mysql#connection-options
   */
  constructor(driverName, driverConfig) {
    this.#supportedDrivers = ['sqlite3', 'redis', 'mysql'];

    if (!this.#supportedDrivers.includes(driverName)) {
      throw new Error(`Driver not supported: ${driverName}`);
    }

    const drivers = {};

    drivers.sqlite3 = () => {
      if (driverConfig.filePath === ':memory:') {
        throw new Error(':memory: is not supported');
      }

      const driver = new Sqlite3Driver(
        util.promisify,
        getCurrentTimestamp,
        sqlite3,
        driverConfig
      );
      this.#queueClient = new QueueClient(driver, uuidGenerator, getCurrentTimestamp, new Worker());
    };

    drivers.redis = () => {
      const driver = new RedisDriver(
        getCurrentTimestamp,
        redis,
        driverConfig,
        RedLock
      );

      this.#queueClient = new QueueClient(driver, uuidGenerator, getCurrentTimestamp, new Worker());
    };

    drivers.mysql = () => {
      const driver = new MysqlDriver(
        getCurrentTimestamp,
        mysql,
        driverConfig
      );

      this.#queueClient = new QueueClient(driver, uuidGenerator, getCurrentTimestamp, new Worker());
    };

    drivers[driverName]();
  }

  /**
   * Creates the necessary database structure for jobs (applicable to SQL drivers).
   * @returns {Promise<void>}
   */
  async createJobsDbStructure() {
    await this.#queueClient.createJobsDbStructure();
  }

  /**
   * Pushes a new job to a queue.
   * @param {Object} payload - The payload of the job.
   * @param {string} [queue=default] - The name of the queue.
   * @returns {Promise<string>} - A promise resolving to the UUID of the created job.
   *
   * @example
   * const jobUuid = await verySimpleQueue.pushJob({ sendEmailTo: 'foo@foo.com' }, 'emails-to-send');
   */
  async pushJob(payload, queue = 'default') {
    return this.#queueClient.pushJob(payload, queue);
  }

  /**
   * Stores a page crawler hit
   * @param {Object} payload - The payload of the crawler hit.
   * @returns {Promise<string>} - A promise resolving to the UUID of the created job.
   */
  async storeCrawlerHit(payload) {
    return this.#queueClient.storeCrawlerHit(payload);
  }

  /**
 * Retrieves crawler hits.
 *
 * @param {number} pageNumber - The page number to retrieve.
 * @param {number} pageSize - The number of items per page.
 * @returns {Promise<Array>} A promise resolving to an array of crawler hits.
 */
  async getCrawlerHits(pageNumber, pageSize) {
    return this.#queueClient.getCrawlerHits(pageNumber, pageSize);
  }

  /**
   * Handles one job from the specified queue.
   * @param {module:types.JobHandler} jobHandler - The function to handle the job.
   * @param {string} [queue=default] - The name of the queue.
   * @param {boolean} [throwErrorOnFailure=false] - If true, mark the job as failed and throw an error on failure.
   * @returns {Promise<*>} - A promise resolving to the result of the job handler function.
   *
   * @example
   * await verySimpleQueue.handleJob((payload) => sendEmail(payload.email), 'emails-to-send');
   */
  async handleJob(jobHandler, queue = 'default', throwErrorOnFailure = false) {
    return this.#queueClient.handleJob(jobHandler, queue, throwErrorOnFailure);
  }

  /**
   * Handles a job specified by its UUID.
   * @param {module:types.JobHandler} jobHandler - The function to handle the job.
   * @param {string} jobUuid - The UUID of the job.
   * @param {boolean} [throwErrorOnFailure=false] - If true, mark the job as failed and throw an error on failure.
   * @returns {Promise<*>} - A promise resolving to the result of the job handler function.
   *
   * @example
   * await verySimpleQueue.handleJobByUuid(
   *   (payload) => sendEmail(payload.email),
   *   'd5dfb2d6-b845-4e04-b669-7913bfcb2600'
   * );
   */
  async handleJobByUuid(jobHandler, jobUuid, throwErrorOnFailure = false) {
    return this.#queueClient.handleJobByUuid(jobHandler, jobUuid, throwErrorOnFailure);
  }

  /**
   * Enqueues a job specified by its UUID.
   * @param {string} jobUuid - The UUID of the job.
   * @returns {Promise<*>}
   *
   * @example
   * await verySimpleQueue.enqueueJobByUuid('d5dfb2d6-b845-4e04-b669-7913bfcb2600');
   */
  async enqueueJobByUuid(jobUuid) {
    return this.#queueClient.enqueueJobByUuid(jobUuid);
  }

  /**
   * Handles a finished job specified by its UUID.
   * @param {string} jobUuid - The UUID of the finished job.
   * @returns {Promise<*>}
   *
   * @example
   * await verySimpleQueue.handleFinishedJobByUuid('d5dfb2d6-b845-4e04-b669-7913bfcb2600');
   */
  async handleFinishedJobByUuid(jobUuid) {
    return this.#queueClient.handleFinishedJobByUuid(jobUuid);
  }

  /**
   * Handles a failed job in the specified queue.
   * @param {module:types.JobHandler} jobHandler - The function to handle the job.
   * @param {string} [queue=default] - The name of the queue.
   * @param {boolean} [throwErrorOnFailure=false] - If true, mark the job as failed and throw an error on failure.
   * @returns {Promise<*>} - A promise resolving to the result of the job handler function.
   *
   * @example
   * await verySimpleQueue.handleFailedJob((payload) => tryAgain(payload.email), 'emails-to-send');
   */
  async handleFailedJob(jobHandler, queue = 'default', throwErrorOnFailure = false) {
    return this.#queueClient.handleFailedJob(jobHandler, queue, throwErrorOnFailure);
  }

  /**
   * Closes the connection to the database.
   * @returns {Promise<void>}
   */
  async closeConnection() {
    await this.#queueClient.closeConnection();
  }

  /**
   * Starts the worker to process jobs.
   * @param {module:types.JobHandler} jobHandler - The function to handle the jobs.
   * @param {module:types.WorkerSettings} settings - The settings for the worker.
   * @returns {Promise<void>}
   *
   * @example
   * await verySimpleQueue.work(
   *   (payload) => sendEmail(payload.email),
   *   { queue: 'email-to-send' }
   * );
   */
  async work(jobHandler, settings) {
    await this.#queueClient.work(jobHandler, settings);
  }

  /**
   * Signals the workers to stop working after they have finished with the current job.
   * @returns {void}
   */
  shutdown() {
    this.#queueClient.shutdown();
  }

  /************** Custom Methods **************/

  /**
   * Retrieves all jobs in the specified queue.
   * @param {string} [queue=default] - The name of the queue.
   * @returns {Promise<Array>} - A promise resolving to an array of jobs.
   */
  async getAllJobsByQueue(queue = 'default') {
    return this.#queueClient.getAllJobsByQueue(queue);
  }

  /**
   * Retrieves all finished jobs in the specified queue.
   * @param {string} [queue=default] - The name of the queue.
   * @returns {Promise<Array>} - A promise resolving to an array of finished jobs.
   */
  async getFinishedJobsByQueue(queue = 'default') {
    return this.#queueClient.getFinishedJobsByQueue(queue);
  }

  /**
   * Enqueues all reserved jobs in the specified queue.
   * @param {string} [queue=default] - The name of the queue.
   * @returns {Promise<void>}
   */
  async enqueueAllReservedJobs(queue = 'default') {
    return this.#queueClient.enqueueAllReservedJobs(queue);
  }

  /**
   * Checks if the workers should be shut down.
   * @returns {boolean} - True if the workers should be shut down, false otherwise.
   */
  shouldShutdown() {
    return this.#queueClient.shouldShutdown();
  }

  /**
   * Turns on the worker.
   * @returns {void}
   */
  turnOn() {
    return this.#queueClient.turnOn();
  }
}

module.exports = VerySimpleQueue;
