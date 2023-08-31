const uuidGenerator = require('uuid').v4;

/**
 * MysqlDriver class for managing jobs using MySQL.
 * @class
 * @implements Driver
 */
class MysqlDriver {
  #parseJobResult;
  #getNewConnection;
  #run;
  #runListQuery;
  #runWithConnection;
  #parseQueryResults;
  #reserveJob;
  #getCurrentTimestamp;

  /**
   * Creates an instance of MysqlDriver.
   * @param {module:helpers.getCurrentTimestamp} getCurrentTimestamp - Function to get the current timestamp.
   * @param {Object} mysql - MySQL module.
   * @param {Object} driverConfig - Configuration for the MySQL driver.
   */
  constructor(getCurrentTimestamp, mysql, driverConfig) {
    this.#getCurrentTimestamp = getCurrentTimestamp;

    this.#parseJobResult = (result) => {
      if (!result) {
        return null;
      }

      const job = result;
      job.payload = JSON.parse(job.payload);

      return job;
    };

    this.#getNewConnection = async () => mysql.createConnection(driverConfig);

    this.#parseQueryResults = (results) => {
      if (!results) {
        return null;
      }

      if (results.length === 0) {
        return null;
      }

      if (results[0].length === 1) {
        return results[0][0];
      }

      return results[0];
    };

    this._parseListQueryResult = (result) => {
      const resultSet = result[0];

      if (Array.isArray(resultSet)) {
        return resultSet;
      } else if (typeof resultSet === 'object' && resultSet !== null) {
        if (Array.isArray(resultSet.rows)) {
          return resultSet.rows;
        } else {
          return [resultSet];
        }
      } else {
        return [];
      }
    };

    this.#runListQuery = async (query, params = []) => {
      const connection = await this.#getNewConnection();
      const results = params.length === 0
        ? await connection.query(query)
        : await connection.execute(query, params);

      await connection.end();
      const result = this._parseListQueryResult(results);
      return result;
    };

    this.#run = async (query, params = []) => {
      const connection = await this.#getNewConnection();
      const results = params.length === 0
        ? await connection.query(query)
        : await connection.execute(query, params);

      await connection.end();
      const result = this.#parseQueryResults(results);
      return result;
    };

    this.#runWithConnection = async (connection, query, params = []) => {
      const results = params.length === 0
        ? await connection.query(query)
        : await connection.execute(query, params);

      return this.#parseQueryResults(results);
    };

    this.#reserveJob = async (selectQuery, params) => {
      const connection = await this.#getNewConnection();
      try {
        await this.#runWithConnection(connection, 'START TRANSACTION;', []);
        const rawJob = await this.#runWithConnection(connection, `${selectQuery} FOR UPDATE`, params);

        if (!rawJob) {
          await this.#runWithConnection(connection, 'COMMIT', []);
          await connection.end();
          return null;
        }

        const job = this.#parseJobResult(rawJob);
        const timestamp = this.#getCurrentTimestamp();
        await this.#runWithConnection(connection, `UPDATE jobs SET reserved_at = ${timestamp} WHERE uuid = "${job.uuid}"`, []);
        await this.#runWithConnection(connection, 'COMMIT', []);
        await connection.end();
        return job;
      } catch (error) {
        await this.#runWithConnection(connection, 'ROLLBACK', []);
        await connection.end();
        return null;
      }
    };
  }

  async createJobsDbStructure() {
    const queryJobs = 'CREATE TABLE IF NOT EXISTS jobs('
      + 'uuid CHAR(36) PRIMARY KEY,'
      + 'queue TEXT NOT NULL,'
      + 'payload TEXT NOT NULL,'
      + 'created_at INTEGER UNSIGNED NOT NULL,'
      + 'reserved_at INTEGER UNSIGNED NULL,'
      + 'failed_at INTEGER UNSIGNED NULL,'
      + 'completed_at INT(10) UNSIGNED NULL,'
      + 'domain VARCHAR(255) NOT NULL,' // New column: domain
      + 'cache_time INT NULL,' // New column: cache_time
      + 'cached_at INTEGER UNSIGNED NULL,' // New column: cached_at
      + 'is_cached TINYINT(1) NULL,' // New column: is_cached
      + 'http_status INT NULL,' // New column: http_status
      + 'KEY idx_payload (payload(255))'
      + ') ENGINE=InnoDB DEFAULT CHARSET=utf8;';

    await this.#run(queryJobs);

    const queryHistorical = 'CREATE TABLE IF NOT EXISTS historical_jobs('
      + 'uuid CHAR(36) PRIMARY KEY,'
      + 'queue TEXT NOT NULL,'
      + 'payload TEXT NOT NULL,'
      + 'created_at INTEGER UNSIGNED NOT NULL,'
      + 'completed_at INT(10) UNSIGNED NULL,'
      + 'domain VARCHAR(255) NOT NULL,' // New column: domain
      + 'cache_time INT NULL,' // New column: cache_time
      + 'cached_at INTEGER UNSIGNED NULL,' // New column: cached_at
      + 'is_cached TINYINT(1) NULL,' // New column: is_cached
      + 'http_status INT NULL,' // New column: http_status
      + 'KEY idx_payload (payload(255))'
      + ') ENGINE=InnoDB DEFAULT CHARSET=utf8;';

    await this.#run(queryHistorical);

    const queryCrawlerHits = `
    CREATE TABLE IF NOT EXISTS crawler_hits (
        uuid CHAR(36) PRIMARY KEY,
        url VARCHAR(255) NOT NULL,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        visited_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        bot VARCHAR(255),
        http_status INT,
        time_to_render INT,
        cache_hit TINYINT(1),
        KEY idx_url (uuid, url),
        INDEX idx_single_url (url)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
`;

    await this.#run(queryCrawlerHits);
  }

  /**
 * Stores a crawler hit in the crawler_hits table.
 * @param {module:types.CrawlerHit} crawlerHit - The crawler hit to store.
 * @returns {Promise<void>}
 */
  async storeCrawlerHit(crawlerHit) {
    console.log('--storeCrawlerHit driver: ', crawlerHit);

    const query = 'INSERT INTO crawler_hits(uuid, url, bot, http_status, time_to_render, cache_hit) VALUES (?, ?, ?, ?, ?, ?)';
    await this.#run(query, [
      crawlerHit.uuid,
      crawlerHit.url,
      crawlerHit.bot,
      crawlerHit.http_status,
      crawlerHit.time_to_render,
      crawlerHit.cache_hit
    ]);
  }

  /**
   * Stores a job in the jobs table.
   * @param {module:types.Job} job - The job to store.
   * @returns {Promise<void>}
   */
  async storeJob(job) {
    const query = 'INSERT INTO jobs(uuid, queue, payload, created_at, domain, cache_time, cached_at, is_cached, http_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)';
    await this.#run(query, [
      job.uuid,
      job.queue,
      JSON.stringify(job.payload),
      job.created_at,
      job.domain, // Add the domain column value
      job.cache_time, // Add the cache_time column value
      job.cached_at, // Add the cached_at column value
      job.is_cached, // Add the is_cached column value
      job.http_status // Add the http_status column value
    ]);
  }

  /**
   * Stores a finished job in the jobs table.
   * @param {module:types.Job} job - The finished job to store.
   * @returns {Promise<void>}
   */
  async storeFinishedJob(job) {
    // console.log('--storeFinishedJob: ', job);
    const query = 'INSERT INTO historical_jobs(uuid, queue, payload, created_at, completed_at, domain, cache_time, cached_at, is_cached, http_status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';
    await this.#run(query, [
      uuidGenerator(),
      job.queue,
      JSON.stringify(job.payload),
      job.created_at,
      job.completed_at,
      job.domain, // Add the domain column value
      job.cache_time, // Add the cache_time column value
      job.cached_at, // Add the cached_at column value
      job.is_cached, // Add the is_cached column value
      job.http_status // Add the http_status column value
    ]);
  }

  /**
   * Retrieves the next available job from the specified queue.
   * @param {string} queue - The queue from which to get the job.
   * @returns {Promise<module:types.Job|null>} - A promise resolving to the next available job, or null if none is available.
   */
  async getJob(queue) {
    const query = 'SELECT * FROM jobs WHERE queue = ? AND failed_at IS NULL AND reserved_at IS NULL ORDER BY created_at DESC LIMIT 1';
    return this.#reserveJob(query, [queue]);
  }

  /**
   * Retrieves a job with the specified UUID.
   * @param {string} jobUuid - The UUID of the job.
   * @returns {Promise<module:types.Job|null>} - A promise resolving to the job with the specified UUID, or null if not found.
   */
  async getJobByUuid(jobUuid) {
    const query = 'SELECT * FROM jobs WHERE uuid = ? LIMIT 1';
    return this.#run(query, [jobUuid]);
  }

  /**
   * Retrieves a finished job with the specified UUID.
   * @param {string} jobUuid - The UUID of the job.
   * @returns {Promise<module:types.Job|null>} - A promise resolving to the job with the specified UUID, or null if not found.
   */
  async getFinishedJobByUuid(jobUuid) {
    const query = 'SELECT * FROM historical_jobs WHERE uuid = ? LIMIT 1';
    return this.#run(query, [jobUuid]);
  }

  /**
   * Updates the specified job with the provided values.
   * @param {Object} updateOptions - The options to update the job (created_at, reserved_at, failed_at, uuid).
   * @returns {Promise<module:types.Job|null>} - A promise resolving to the updated job, or null if not found.
   */
  updateJobByUuid({
    created_at = null,
    reserved_at = null,
    failed_at = null,
    uuid,
    domain = null,
    cache_time = null,
    cached_at = null,
    is_cached = null,
    http_status = null
  }) {
    const query = 'UPDATE jobs SET created_at = ?, reserved_at = ?, failed_at = ?, domain = ?, cache_time = ?, cached_at = ?, is_cached = ?, http_status = ? WHERE uuid = ?';
    return this.#run(query, [created_at, reserved_at, failed_at, domain, cache_time, cached_at, is_cached, http_status, uuid]);
  }

  /**
   * Retrieves a job with the specified payload.
   * @param {string} jobPayload - The payload of the job.
   * @returns {Promise<module:types.Job|null>} - A promise resolving to the job with the specified payload, or null if not found.
   */
  async getJobByPayload(jobPayload) {
    const query = 'SELECT * FROM jobs WHERE payload = ?';
    return this.#run(query, [jobPayload]);
  }

  /**
   * Retrieves the next available failed job from the specified queue.
   * @param {string} queue - The queue from which to get the failed job.
   * @returns {Promise<module:types.Job|null>} - A promise resolving to the next available failed job, or null if none is available.
   */
  async getFailedJob(queue) {
    const query = 'SELECT * FROM jobs WHERE queue = ? AND failed_at IS NOT NULL LIMIT 1';
    return this.#reserveJob(query, [queue]);
  }

  /**
   * Deletes a job with the specified UUID.
   * @param {string} jobUuid - The UUID of the job to delete.
   * @returns {Promise<void>}
   */
  async deleteJob(jobUuid) {
    await this.#run('DELETE FROM jobs WHERE reserved_at IS NOT NULL AND uuid = ?', [jobUuid]);
  }

  /**
   * Deletes a job with the specified UUID.
   * @param {string} jobUuid - The UUID of the job to delete.
   * @returns {Promise<void>}
   */
  async deleteFinishedJob(jobUuid) {
    await this.#run('DELETE FROM jobs WHERE reserved_at IS NOT NULL AND uuid = ?', [jobUuid]);
  }

  /**
   * Marks a job with the specified UUID as failed.
   * @param {string} jobUuid - The UUID of the job to mark as failed.
   * @returns {Promise<void>}
   */
  async markJobAsFailed(jobUuid, status) {
    const timestamp = this.#getCurrentTimestamp();
    await this.#run('UPDATE jobs SET failed_at = ?, reserved_at = NULL, http_status = ?, is_cached = false WHERE uuid = ?', [timestamp, status, jobUuid]);
  }

  /**
   * Deletes all jobs.
   * @returns {Promise<void>}
   */
  async deleteAllJobs() {
    await this.#run('DELETE * FROM jobs');
  }

  /**
   * Closes the database connection.
   * @returns {Promise<void>}
   */
  async closeConnection() {
    // Add code to close the database connection here
  }

  /************ Custom Methods **************/

  /**
   * Retrieves all jobs in the specified queue.
   * @param {string} queue - The name of the queue.
   * @returns {Promise<Array>} - A promise resolving to an array of jobs.
   */
  async getAllJobsByQueue(queue) {
    const query = 'SELECT * FROM jobs WHERE queue = ? AND completed_at IS NULL';
    return this.#runListQuery(query, [queue]);
  }

  /**
   * Retrieves all finished jobs in the specified queue.
   * @param {string} queue - The name of the queue.
   * @returns {Promise<Array>} - A promise resolving to an array of finished jobs.
   */
  async getFinishedJobsByQueue(queue) {
    const query = 'SELECT * FROM historical_jobs WHERE queue = ? AND completed_at IS NOT NULL ORDER BY completed_at DESC';
    return this.#runListQuery(query, [queue]);
  }

  /**
   * Enqueues all reserved jobs in the specified queue.
   * @param {string} queue - The name of the queue.
   * @returns {Promise<void>}
   */
  async enqueueAllReservedJobs(queue) {
    const query = `UPDATE jobs SET reserved_at = NULL, failed_at = NULL WHERE queue = ? AND reserved_at IS NOT NULL AND reserved_at < UNIX_TIMESTAMP()`;
    return await this.#run(query, [queue]);
  }
}

module.exports = MysqlDriver;
