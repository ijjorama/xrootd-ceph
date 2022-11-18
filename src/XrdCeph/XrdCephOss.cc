//------------------------------------------------------------------------------
// Copyright (c) 2014-2015 by European Organization for Nuclear Research (CERN)
// Author: Sebastien Ponce <sebastien.ponce@cern.ch>
//------------------------------------------------------------------------------
// This file is part of the XRootD software suite.
//
// XRootD is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// XRootD is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with XRootD.  If not, see <http://www.gnu.org/licenses/>.
//
// In applying this licence, CERN does not waive the privileges and immunities
// granted to it by virtue of its status as an Intergovernmental Organization
// or submit itself to any jurisdiction.
//------------------------------------------------------------------------------

#include <stdio.h>
#include <string>
#include <fcntl.h>
#include <set>

#include "XrdCeph/XrdCephPosix.hh"
#include "XrdOuc/XrdOucEnv.hh"
#include "XrdSys/XrdSysError.hh"
#include "XrdOuc/XrdOucTrace.hh"
#include "XrdOuc/XrdOucStream.hh"
#include "XrdOuc/XrdOucName2Name.hh"
#ifdef XRDCEPH_SUBMODULE
#include "XrdOuc/XrdOucN2NLoader.hh"
#else
#include "private/XrdOuc/XrdOucN2NLoader.hh"
#endif
#include "XrdVersion.hh"
#include "XrdCeph/XrdCephOss.hh"
#include "XrdCeph/XrdCephOssDir.hh"
#include "XrdCeph/XrdCephOssFile.hh"

XrdVERSIONINFO(XrdOssGetStorageSystem, XrdCephOss);

XrdSysError XrdCephEroute(0);
XrdOucTrace XrdCephTrace(&XrdCephEroute);

static void (*g_logfunc) (char *, ...) = 0;

static void logwrapper(char* format, ...) {
  if (0 == g_logfunc) return;
  va_list arg;
  va_start(arg, format);
  (*g_logfunc)(format, arg);
  va_end(arg);
}

void ceph_posix_set_logfunc(void (*logfunc) (char *, ...)) {
  g_logfunc = logwrapper;
};
/// pointer to library providing Name2Name interface. 0 be default
/// populated in case of ceph.namelib entry in the config file
/// used in XrdCephPosix
extern XrdOucName2Name *g_namelib;

ssize_t getNumericAttr(const char* path, const char* attrName, const int maxAttrLen)
{

  char *attrValue = (char*)malloc(maxAttrLen+1);
  ssize_t attrLen = ceph_posix_getxattr((XrdOucEnv*)NULL, path, attrName, attrValue, maxAttrLen);

  if (attrLen <= 0) {

    return attrLen;

  } else {

    attrValue[attrLen] = (char)NULL;
    char *endPointer;
    return strtoll(attrValue, &endPointer, 10);

  }
}

extern "C"
{
  XrdOss*
  XrdOssGetStorageSystem(XrdOss* native_oss,
                         XrdSysLogger* lp,
                         const char* config_fn,
                         const char* parms)
  {
    // Do the herald thing
    XrdCephEroute.SetPrefix("ceph_");
    XrdCephEroute.logger(lp);
    XrdCephEroute.Say("++++++ CERN/IT-DSS XrdCeph");
    // set parameters
    try {
      ceph_posix_set_defaults(parms);
    } catch (std::exception &e) {
      XrdCephEroute.Say("CephOss loading failed with exception. Check the syntax of parameters : ", parms);
      return 0;
    }
    // deal with logging
    ceph_posix_set_logfunc(logwrapper);
    return new XrdCephOss(config_fn, XrdCephEroute);
  }
}

XrdCephOss::XrdCephOss(const char *configfn, XrdSysError &Eroute) {
  Configure(configfn, Eroute);
}

XrdCephOss::~XrdCephOss() {
  ceph_posix_disconnect_all();
}

// declared and used in XrdCephPosix.cc
extern unsigned int g_maxCephPoolIdx;
int XrdCephOss::Configure(const char *configfn, XrdSysError &Eroute) {
   int NoGo = 0;
   XrdOucEnv myEnv;
   XrdOucStream Config(&Eroute, getenv("XRDINSTANCE"), &myEnv, "=====> ");
   //disable posc  
   XrdOucEnv::Export("XRDXROOTD_NOPOSC", "1");
   // If there is no config file, nothing to be done
   if (configfn && *configfn) {
     // Try to open the configuration file.
     int cfgFD;
     if ((cfgFD = open(configfn, O_RDONLY, 0)) < 0) {
       Eroute.Emsg("Config", errno, "open config file", configfn);
       return 1;
     }
     Config.Attach(cfgFD);
     // Now start reading records until eof.
     char *var;
     while((var = Config.GetMyFirstWord())) {
       if (!strncmp(var, "ceph.nbconnections", 18)) {
         var = Config.GetWord();
         if (var) {
           unsigned long value = strtoul(var, 0, 10);
           if (value > 0 and value <= 100) {
             g_maxCephPoolIdx = value;
           } else {
             Eroute.Emsg("Config", "Invalid value for ceph.nbconnections in config file (must be between 1 and 100)", configfn, var);
             return 1;
           }
         } else {
           Eroute.Emsg("Config", "Missing value for ceph.nbconnections in config file", configfn);
           return 1;
         }
       }
       if (!strncmp(var, "ceph.namelib", 12)) {
         var = Config.GetWord();
         if (var) {
           std::string libname = var;
           // Warn in case parameters were givne
           char parms[1040];
           bool hasParms{false};
           if (!Config.GetRest(parms, sizeof(parms)) || parms[0]) {
              hasParms = true;
           }
           // Load name lib
           XrdOucN2NLoader  n2nLoader(&Eroute,configfn,(hasParms?parms:""),NULL,NULL);
           g_namelib = n2nLoader.Load(libname.c_str(), XrdVERSIONINFOVAR(XrdOssGetStorageSystem), NULL);
           if (!g_namelib) {
             Eroute.Emsg("Config", "Unable to load library given in ceph.namelib : %s", var);
           }
         } else {
           Eroute.Emsg("Config", "Missing value for ceph.namelib in config file ", configfn);
           return 1;
         }
       }
     }

     // Now check if any errors occured during file i/o
     int retc = Config.LastError();
     if (retc) {
       NoGo = Eroute.Emsg("Config", -retc, "read config file",
                          configfn);
     }
     Config.Close();
   }
   return NoGo;
}

int XrdCephOss::Chmod(const char *path, mode_t mode, XrdOucEnv *envP) {
  return -ENOTSUP;
}

int XrdCephOss::Create(const char *tident, const char *path, mode_t access_mode,
                    XrdOucEnv &env, int Opts) {
  return -ENOTSUP;
}

int XrdCephOss::Init(XrdSysLogger *logger, const char* configFn) { return 0; }

//SCS - lie to posix-assuming clients about directories [fixes brittleness in GFAL2]
int XrdCephOss::Mkdir(const char *path, mode_t mode, int mkpath, XrdOucEnv *envP) {
  return 0;
}

//SCS - lie to posix-assuming clients about directories [fixes brittleness in GFAL2]
int XrdCephOss::Remdir(const char *path, int Opts, XrdOucEnv *eP) {
  return 0;
}

int XrdCephOss::Rename(const char *from,
                    const char *to,
                    XrdOucEnv *eP1,
                    XrdOucEnv *eP2) {
  return -ENOTSUP;
}

int XrdCephOss::Stat(const char* path,
                  struct stat* buff,
                  int opts,
                  XrdOucEnv* env) {

  XrdCephEroute.Say("Entering Stat");
  try {
    if (!strcmp(path, "/")) {
      // special case of a stat made by the locate interface
      // we intend to then list all files
      memset(buff, 0, sizeof(*buff));
      buff->st_mode = S_IFDIR | 0700;
      return 0;
    
    } else {
      return ceph_posix_stat(env, path, buff);
    }
  } catch (std::exception &e) {
    XrdCephEroute.Say("stat : invalid syntax in file parameters");
    return -EINVAL;
  }
}

int XrdCephOss::StatFS(const char *path, char *buff, int &blen, XrdOucEnv *eP) {
  
  XrdCephEroute.Say("Entering StatFS");
  XrdOssVSInfo sP;
  int rc = StatVS(&sP, 0, 0);
  if (rc) {
    return rc;
  }
  int percentUsedSpace = (sP.Usage*100)/sP.Total;
  blen = snprintf(buff, blen, "%d %lld %d %d %lld %d",
                  1, sP.Free, percentUsedSpace, 0, 0LL, 0);
  return XrdOssOK;
}

int XrdCephOss::StatVS(XrdOssVSInfo *sP, const char *sname, int updt) {

  XrdCephEroute.Say("Entering StatVS");
  int rc = ceph_posix_statfs(&(sP->Total), &(sP->Free));
  if (rc) {
    return rc;
  }
  sP->Large = sP->Total;
  sP->LFree = sP->Free;
  sP->Usage = sP->Total-sP->Free;
  sP->Extents = 1;
  return XrdOssOK;
}

int formatStatLSResponse(char *buff, int &blen, 
const char* cgroup, long long totalSpace, long long usedSpace, long long freeSpace, long long quota, long long maxFreeChunk)
{
  return snprintf(buff, blen, "oss.cgroup=%s&oss.space=%lld&oss.free=%lld&oss.maxf=%lld&oss.used=%lld&oss.quota=%lld",
                                     cgroup,       totalSpace,    freeSpace,    maxFreeChunk, usedSpace,    quota);
}
int authorizePool(const char *pool)
{

  std::set<std::string> allowablePools({"alice:", "dteam:"});
  return allowablePools.find(pool) != allowablePools.end(); 

}
const char *createAttrPath(const char *pool, const char *attrName) 
{

  return (std::string(pool) + std::string(attrName)).c_str();

}
const char *trimLast(const char *in)
{
  std::string inStr(in);
  return inStr.erase(inStr.length()-1).c_str();

}

int XrdCephOss::StatLS(XrdOucEnv &env, const char *pool, char *buff, int &blen)
{

  if (!authorizePool(pool)) {
    return -EPERM;
  }

  const char *newPool = trimLast(pool); // Remove trailing colon ':' for lib_rados calls to follow
  long long usedSpace, totalSpace, freeSpace;

  if (ceph_posix_stat_pool(newPool, &usedSpace) != 0) {
      return -EINVAL;
  }
  
  const char *spaceInfoPath = createAttrPath(pool, (const char *)"__spaceinfo__");

  totalSpace = getNumericAttr(spaceInfoPath, "total_space", 24);
  if (totalSpace < 0) {
    XrdCephEroute.Say("Could not get totalSpace");
    return -EINVAL;
  }

  freeSpace = totalSpace - usedSpace;
  blen = formatStatLSResponse(buff, blen, 
    "default",  /* "oss.cgroup" */ 
    totalSpace, /* "oss.space"  */
    usedSpace,  /* "oss.used"   */
    freeSpace,  /* "oss.free"   */
    totalSpace, /* "oss.quota"  */
    freeSpace   /* "oss.maxf"   */);

  return XrdOssOK;

}
 
int XrdCephOss::Truncate (const char* path,
                          unsigned long long size,
                          XrdOucEnv* env) {
  try {
    return ceph_posix_truncate(env, path, size);
  } catch (std::exception &e) {
    XrdCephEroute.Say("truncate : invalid syntax in file parameters");
    return -EINVAL;
  }
}

int XrdCephOss::Unlink(const char *path, int Opts, XrdOucEnv *env) {
  try {
    return ceph_posix_unlink(env, path);
  } catch (std::exception &e) {
    XrdCephEroute.Say("unlink : invalid syntax in file parameters");
    return -EINVAL;
  }
}

XrdOssDF* XrdCephOss::newDir(const char *tident) {
  return new XrdCephOssDir(this);
}

XrdOssDF* XrdCephOss::newFile(const char *tident) {
  return new XrdCephOssFile(this);
}

