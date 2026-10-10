package RDDAssignment

import java.util.UUID
import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.StringUtils
import utils.{Commit, File, Stats}

object RDDAssignment {


  /**
    * Reductions are often used in data processing in order to gather more useful data out of raw data. In this case
    * we want to know how many commits a given RDD contains.
    *
    * @param commits RDD containing commit data.
    * @return Long indicating the number of commits in the given RDD.
    */
  def assignment_1(commits: RDD[Commit]): Long = commits.count

  /**
    * We want to know how often programming languages are used in committed files. We want you to return an RDD containing Tuples
    * of the used file extension, combined with the number of occurrences. If no filename or file extension is used we
    * assume the language to be 'unknown'.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing tuples indicating the programming language (extension) and number of occurrences.
    */
    private def fileToFileType(file : File):String ={
      if(file.filename.isEmpty) return "unknown"
      val fileName = file.filename.get
      if(!fileName.matches(".*\\..*")) return "unknown"
      fileName.replaceFirst("^.*\\.", "")
    }

  def assignment_2(commits: RDD[Commit]): RDD[(String, Long)] = {
    val rddFiles = commits.flatMap((c : Commit) => c.files)
     rddFiles.map(f => fileToFileType(f)).map(x => (x,1L)).reduceByKey((acc, x) => acc + x)
  }

  /**
    * Competitive users on GitHub might be interested in their ranking in the number of commits. We want you to return an
    * RDD containing Tuples of the rank (zero indexed) of a commit author, a commit author's name and the number of
    * commits made by the commit author. As in general with performance rankings, a higher performance means a better
    * ranking (0 = best). In case of a tie, the lexicographical ordering of the usernames should be used to break the
    * tie.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the rank, the name and the total number of commits for every author, in the ordered fashion.
    */
  def assignment_3(commits: RDD[Commit]): RDD[(Long, String, Long)] = {
    commits.map(c => c.commit.author.name).map(x => (x,1L)).reduceByKey((acc, x) => acc + x).map( n => (- n._2 ,n._1)).sortBy(N => (N._1, N._2.toLowerCase)).zipWithIndex().map(x => (x._2,x._1._2,- x._1._1))
  }

  /**
    * Some users are interested in seeing an overall contribution of all their work. For this exercise we want an RDD that
    * contains the committer's name and the total number of their commit statistics. As stats are Optional, missing Stats cases should be
    * handled as "Stats(0, 0, 0)".
    *
    * Note that if a user is given that is not in the dataset, then the user's name should not occur in
    * the resulting RDD.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing committer names and an aggregation of the committers Stats.
    */
  def assignment_4(commits: RDD[Commit], users: List[String]): RDD[(String, Stats)] = {
    commits.filter(c => users.contains(c.commit.author.name)).map(c => (c.commit.author.name, c.stats.getOrElse(Stats(0,0,0)))).reduceByKey((acc, x) => Stats(acc.total + x.total, acc.additions + x.additions, acc.deletions + x.deletions))
  }


  /**
    * There are different types of people: those who own repositories, and those who make commits. Although Git blame command is
    * excellent in finding these types of people, we want to do it in Spark. As the output, we require an RDD containing the
    * names of commit authors and repository owners that have either exclusively committed to repositories, or
    * exclusively own repositories in the given commits RDD.
    *
    * Note that the repository owner is contained within GitHub URLs.
    *
    * @param commits RDD containing commit data.
    * @return RDD of Strings representing the usernames that have either only committed to repositories or only own
    *         repositories.
    */
  def assignment_5(commits: RDD[Commit]): RDD[String] = {
    val owners = commits.map(c => c.url.split("/")(4))
    val commiters = commits.map(c => c.commit.author.name)
    val union = owners.union(commiters)
    val intersection = owners.intersection(commiters)
    union.subtract(intersection)
  }

  /**
    * Sometimes developers make mistakes and sometimes they make many many of them. One way of observing mistakes in commits is by
    * looking at so-called revert commits. We define a 'revert streak' as the number of times `Revert` occurs
    * in a commit message. Note that for a commit to be eligible for a 'revert streak', its message must start with `Revert`.
    * As an example: `Revert "Revert ...` would be a revert streak of 2, whilst `Oops, Revert Revert little mistake`
    * would not be a 'revert streak' at all.
    *
    * We require an RDD containing Tuples of the username of a commit author and a Tuple containing
    * the length of the longest 'revert streak' of a user and how often this streak has occurred.
    * Note that we are only interested in the longest commit streak of each author (and its frequency).
    *
    * @param commits RDD containing commit data.
    * @return RDD of Tuples containing a commit author's name and a Tuple which contains the length of the longest
    *         'revert streak' as well its frequency.
    */

  def reduce6(a :(Int, Int), b :(Int, Int)) :(Int, Int) = {
    if(a._1 == b._1) return (a._1, a._2+b._2)
    if (a._1 > b._1) return (a._1, a._2)
    (b._1, b._2)
  }


  def assignment_6(commits: RDD[Commit]): RDD[(String, (Int, Int))] = {
    commits.map(c => (c.commit.author.name, c.commit.message.toLowerCase)).filter(c => c._2.startsWith("revert")).mapValues(m => m.split("\\W+").takeWhile(_.equals("revert")).length ).map(c => (c._1,(c._2, 1))).reduceByKey((a, b) => reduce6(a, b))
  }


  /**
    * !!! NOTE THAT FROM THIS EXERCISE ON (INCLUSIVE), EXPENSIVE FUNCTIONS LIKE groupBy ARE NO LONGER ALLOWED TO BE USED !!
    *
    * We want to know the number of commits that have been made to each repository contained in the given RDD. Besides the
    * number of commits, we also want to know the unique committers that contributed to each of these repositories.
    *
    * In real life these wide dependency functions are performance killers, but luckily there are better performing alternatives!
    * The automatic graders will check the computation history of the returned RDDs.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples with the repository name, the number of commits made to the repository as
    *         well as the names of the unique committers to this repository.
    */
  def assignment_7(commits: RDD[Commit]): RDD[(String, Long, Iterable[String])] = {
    commits.map(c=> (c.url.split('/')(5),(1L, c.commit.author.name))).aggregateByKey((0L, Nil :List[String]))( (acc: (Long, List[String]), b: (Long, String)) => (acc._1+b._1, b._2::acc._2), (c: (Long, List[String]),d: (Long, List[String])) => (c._1+d._1,c._2::: d._2)).map(i => (i._1,i._2._1,i._2._2))
  }

  /**
    * Return an RDD of Tuples containing the repository name and all the files that are contained in this repository.
    * Note that the file names must be unique, so if a file occurs multiple times (for example, due to removal, or new
    * addition), the newest File object must be returned. As the filenames are an `Option[String]`, discard the
    * files that do not have a filename.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing the files in each repository as described above.
    */
  def assignment_8(commits: RDD[Commit]): RDD[(String, Iterable[File])] = {
    commits.map(c => (c, c.files)).flatMapValues(f => f).filter(i => i._2.filename.isDefined).map(i => ((i._1.url.split('/')(5), i._2.filename.get), (i._2, i._1.commit.committer.date.getTime))).reduceByKey((a, b) => if (a._2 > b._2) a else b).map(f => (f._1._1, f._2._1)).aggregateByKey(Nil :List[File])( (acc: List[File], b: File) => b::acc, (c: List[File],d: List[File]) => c::: d).map(f => (f._1, f._2.toIterable))
  }


  /**
    * For this assignment you are asked to find all the files of a single repository. This is in order to create an
    * overview of each file by creating a Tuple containing the file name, all corresponding commit SHA's,
    * as well as a Stat object representing all the changes made to the file.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples representing a file name, its corresponding commit SHA's and a Stats object
    *         representing the total aggregation of changes for a file.
    */
  def assignment_9(commits: RDD[Commit], repository: String): RDD[(String, Seq[String], Stats)] = {
    commits.filter(c => c.url.split('/')(5).equals(repository)).flatMap(c => c.files).filter(f => f.filename.isDefined&f.sha.isDefined).map(f => (f.filename,(f.sha.get, Stats(f.deletions+f.additions, f.additions, f.deletions)))).aggregateByKey((Nil :List[String],Stats(0,0,0)))( (acc: (List[String], Stats), b: (String, Stats)) => (b._1::acc._1, Stats(acc._2.total+b._2.total, acc._2.additions+b._2.additions, acc._2.deletions+b._2.deletions)), (c: (List[String], Stats),d: (List[String], Stats)) => (c._1::: d._1,Stats(c._2.total+d._2.total, c._2.additions+d._2.additions, c._2.deletions+d._2.deletions))).map(f => (f._1.get, f._2._1, f._2._2)
    )
  }

  /**
    * We want to generate an overview of the work done by a user per repository. For this we want an RDD containing
    * Tuples with the committer's name, the repository name and a `Stats` object containing the
    * total number of additions, deletions and total contribution to this repository.
    * Note that since Stats are optional, the required type is Option[Stat].
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return RDD containing Tuples of the committer's name, the repository name and an `Option[Stat]` object representing additions,
    *         deletions and the total contribution to this repository by this committer.
    */
    def addTwoStats(a: Option[Stats], b: Option[Stats]):Option[Stats] = {
      if(a.isDefined & b.isDefined) return Option.apply(Stats(a.get.total+b.get.total, a.get.additions+b.get.additions, a.get.deletions+b.get.deletions))
      if(a.isDefined & b.isEmpty) return a
      if(b.isDefined) return b
      Option.empty
    }
  def assignment_10(commits: RDD[Commit]): RDD[(String, String, Option[Stats])] = {
    commits.map(c => ((c.commit.committer.name, c.url.split('/')(5)),c.stats)).reduceByKey((a,b) => addTwoStats(a,b)).map(a => (a._1._1, a._1._2, a._2))
  }


  /**
    * Hashing function that computes the md5 hash of a String and returns a Long, representing the most significant bits of the hashed string.
    * It acts as a hashing function for repository name and username.
    *
    * @param s String to be hashed, consecutively mapped to a Long.
    * @return Long representing the MSB of the hashed input String.
    */
  def md5HashString(s: String): Long = {
    val md = MessageDigest.getInstance("MD5")
    val digest = md.digest(s.getBytes)
    val bigInt = new BigInteger(1, digest)
    val hashedString = bigInt.toString(16)
    UUID.nameUUIDFromBytes(hashedString.getBytes()).getMostSignificantBits
  }

  /**
    * Create a bi-directional graph from committer to repositories. Use the `md5HashString` function above to create unique
    * identifiers for the creation of the graph.
    *
    * Spark's GraphX library is actually used in the real world for algorithms like PageRank, Hubs and Authorities, clique finding, etc.
    * However, this is out of the scope of this course and thus, we will not go into further detail.
    *
    * We expect a node for each repository and each committer (based on committer name).
    * We expect an edge from each committer to the repositories that they have committed to.
    *
    * Look into the documentation of Graph and Edge before starting with this exercise.
    * Your vertices must contain information about the type of node: a 'developer' or a 'repository' node.
    * Edges must only exist between repositories and committers.
    *
    * To reiterate, expensive functions such as groupBy are not allowed.
    *
    * @param commits RDD containing commit data.
    * @return Graph representation of the commits as described above.
    */
  def assignment_11(commits: RDD[Commit]): Graph[(String, String), String] = ???
}
