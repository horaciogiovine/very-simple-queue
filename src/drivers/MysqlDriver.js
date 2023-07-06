
/**
 * @class
 * @implements Driver
 */
class MysqlDriver {
  #parseJobResult

  #getNewConnection

  #run

  #runWithConnection

  #parseQueryResults

  #reserveJob

  /** @type module:helpers.getCurrentTimestamp */
  #getCurrentTimestamp

  /**
   * @param {module:helpers.getCurrentTimestamp} getCurrentTimestamp
   * @param {Object} mysql
   * @param {Object} driverConfig
   */
  constructor(getCurrentTimestamp, mysql, driverConfig) {
    this.#getCurrentTimestamp = getCurrentTimestamp;

    /**
     * @param {Object} result
     * @returns {module:types.Job|null}
     */
    this.#parseJobResult = (result) => {
      if (!result) {
        return null;
      }

      const job = result;
      job.payload = JSON.parse(job.payload);

      return job;
    };

    /**
     * @returns {Promise<Object>}
     */
    this.#getNewConnection = async () => mysql.createConnection(driverConfig);

    /**
     * @param {Array|null} results
     * @returns {*|null}
     */
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
    }

    /**
     * @param {string} query
     * @param {Array} [params=[]]
     * @returns {Promise<void>}
     */
    this.#run = async (query, params = []) => {
      const connection = await this.#getNewConnection();

      console.log('--------: ', params);
      const results = params.length === 0
        ? await connection.query(query) : await connection.execute(query, params);

        // console.log('-- connection.query: ', results)
      await connection.end();

      return this.#parseQueryResults(results);
    };

    /**
     * @param {Object} connection
     * @param {string} query
     * @param {Array} [params=[]]
     * @returns {Promise<void>}
     */
    this.#runWithConnection = async (connection, query, params = []) => {
      const results = params.length === 0
        ? await connection.query(query) : await connection.execute(query, params);

      return this.#parseQueryResults(results);
    };

    /**
     * @param {string} selectQuery
     * @param {Array} params
     * @returns {Promise<module:types.Job|null>}
     */
    this.#reserveJob = async (selectQuery, params) => {
      const connection = await this.#getNewConnection();
      try {
        await this.#runWithConnection(connection, 'START TRANSACTION;', []);
        const rawJob = await this.#runWithConnection(connection, `${selectQuery} FOR UPDATE`, params);

        if (!rawJob) {
          await this.#runWithConnection(connection, 'COMMIT', []);
          await connection.end()
          return null;
        }

        const job = this.#parseJobResult(rawJob);
        const timestamp = this.#getCurrentTimestamp();
        await this.#runWithConnection(connection, `UPDATE jobs SET reserved_at = ${timestamp} WHERE uuid = "${job.uuid}"`, []);
        await this.#runWithConnection(connection, 'COMMIT', []);
        await connection.end()
        return job;
      } catch (error) {
        await this.#runWithConnection(connection, 'ROLLBACK', []);
        await connection.end()
        return null;
      }
    };
  }

  /**
   * @returns {Promise<void>}
   */
  async createJobsDbStructure() {
    const query = 'CREATE TABLE IF NOT EXISTS jobs('
      + 'uuid CHAR(36) PRIMARY KEY,'
      + 'queue TEXT NOT NULL,'
      + 'payload TEXT NOT NULL,'
      + 'created_at INTEGER UNSIGNED NOT NULL,'
      + 'reserved_at INTEGER UNSIGNED NULL,'
      + 'failed_at INTEGER UNSIGNED NULL'
      + ')';

    await this.#run(query);
  }

  /**
   * @param {module:types.Job} job
   * @returns {Promise<void>}
   */
  async storeJob(job) {
    const query = 'INSERT INTO jobs(uuid, queue, payload, created_at) VALUES (?, ?, ?, ?)';

    await this.#run(query, [
      job.uuid,
      job.queue,
      JSON.stringify(job.payload),
      job.created_at,
    ]);
  }

  /**
   * @param {string} queue
   * @returns {Promise<module:types.Job|null>}
   */
  async getJob(queue) {
    const query = 'SELECT * FROM jobs WHERE queue = ? AND failed_at IS NULL AND reserved_at IS NULL ORDER BY created_at DESC LIMIT 1';
    return this.#reserveJob(query, [queue]);
  }

  /**
   * @param {string} jobUuid
   * @returns {Promise<module:types.Job|null>}
   */
  async getJobByUuid(jobUuid) {
    const query = 'SELECT * FROM jobs WHERE uuid = ? AND reserved_at IS NULL LIMIT 1';

    return this.#reserveJob(query, [jobUuid]);
  }

  /**
   * @param {string} jobUuid
   * @returns {Promise<module:types.Job|null>}
   */
  async updateJobByUuid({ created_at = null, reserved_at = null, failed_at = null, uuid }) {
    const query = 'UPDATE jobs SET created_at = ?, reserved_at = ?, failed_at = ? WHERE uuid = ?';

    return this.#run(query, [created_at, reserved_at, failed_at, uuid]);
  }

  /**
   * @param {string} jobPayload
   * @returns {Promise<module:types.Job|null>}
   */
  async getJobByPayload(jobPayload) {
    const query = 'SELECT * FROM jobs WHERE payload = ?';

    return this.#run(query, [jobPayload]);
  }

  /**
   * @param {string} queue
   * @returns {Promise<module:types.Job|null>}
   */
  async getFailedJob(queue) {
    const query = 'SELECT * FROM jobs WHERE queue = ? AND failed_at IS NOT NULL AND reserved_at IS NULL LIMIT 1';

    return this.#reserveJob(query, [queue]);
  }

  /**
   * @param {string} jobUuid
   * @returns {Promise<void>}
   */
  async deleteJob(jobUuid) {
    await this.#run('DELETE FROM jobs WHERE reserved_at IS NOT NULL AND uuid = ?', [jobUuid]);
  }

  /**
   * @param {string} jobUuid
   * @returns {Promise<void>}
   */
  async markJobAsFailed(jobUuid) {
    const timestamp = this.#getCurrentTimestamp();
    await this.#run('UPDATE jobs SET failed_at = ?, reserved_at = NULL WHERE uuid = ?', [timestamp, jobUuid]);
  }

  /**
   * @returns {Promise<void>}
   */
  async deleteAllJobs() {
    await this.#run('DELETE FROM jobs');
  }

  /**
   * @returns {Promise<void>}
   */
  async closeConnection() {
  }

  /************ custom code */

  /**
   * @param {string} queue
   * @returns {Promise<module:types.Job|null>}
   */
  async getAllJobsByQueue(queue) {
    const query = 'SELECT * FROM jobs WHERE queue = ?';

    return this.#run(query, [queue]);
  }

  async enqueueAllReservedJobs(queue) {
    return await this.#run(`UPDATE jobs SET reserved_at = NULL, failed_at = NULL WHERE queue = "${queue}" AND reserved_at IS NOT NULL AND reserved_at < UNIX_TIMESTAMP()`);
  }
}

module.exports = MysqlDriver;
