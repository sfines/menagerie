/*
 * Copyright 2010 Scott Fines
 * <p>
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.menagerie;

import java.lang.annotation.*;

/**
 * Used to indicate that a particular feature is still under heavy development and should
 * <i>not</i> be used in production.
 * <p>
 * Using a class marked with this annotation in production and having trouble is should not be considered
 * unexpected. Use these classes <i>AT YOUR OWN RISK</i>
 *
 * @author Scott Fines
 * @version 1.0
 *          Date: 11-Jan-2011
 *          Time: 21:02:12
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface Beta {
}
